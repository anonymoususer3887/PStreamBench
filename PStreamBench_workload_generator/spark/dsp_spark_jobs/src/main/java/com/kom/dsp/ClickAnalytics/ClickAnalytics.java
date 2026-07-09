package com.kom.dsp.ClickAnalytics;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.MetricsUtils;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ClickAnalytics {

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        String[] pd = parallelism.split(",");
        int sourcePar = Integer.parseInt(pd[0]);
        int parserPar = Integer.parseInt(pd[1]);

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int query = Integer.parseInt(params.get("query"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Spark Click Analytics job currently only supports mode=KAFKA");
        }

        SparkSession spark = SparkSession.builder().getOrCreate();

        // Per-stage row counts + end-to-end lag (Tier 3 operator metrics --
        // see MetricsUtils / WordCountJob.java for the full explanation).
        // Queries 1/3 have a separate "reduce" stage; query 2 has "geo"
        // instead (see class javadoc for why these differ).
        if (query == Constants.QUERY_TWO) {
            MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "geo", "sink", "end_to_end_lag");
        } else {
            MetricsUtils.registerOperatorMetrics(spark, "source", "parser", "repeatvisit", "reduce", "sink",
                    "end_to_end_lag");
        }

        Dataset<Row> source = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("subscribe", input)
                .option("startingOffsets", "latest")
                .load();

        // End-to-end lag: see WordCountJob.java for why current_timestamp()
        // must be materialized into its own column before being observed.
        Dataset<Row> sourceLagColumn = source.withColumn("lag_seconds",
                functions.current_timestamp().cast("double").minus(functions.col("timestamp").cast("double")));
        Dataset<Row> sourceWithLag = sourceLagColumn.observe("end_to_end_lag",
                functions.avg(functions.col("lag_seconds")).cast("double").as("rows"));

        // Stage [0]: source.
        Dataset<String> lines = sourceWithLag.selectExpr("CAST(value AS STRING) AS value")
                .as(Encoders.STRING())
                .repartition(sourcePar)
                .observe("source", functions.count(functions.lit(1)).cast("double").as("rows"));

        // Stage [1]: click-log-parser, mirrors ClickLogParser.java
        // (JSON fields url/ip/clientKey).
        StructType clickSchema = new StructType()
                .add("url", "string").add("ip", "string").add("clientKey", "string");
        Dataset<Row> parsed = lines
                .map((org.apache.spark.api.java.function.MapFunction<String, Row>) s -> {
                    com.google.gson.JsonObject obj = new com.google.gson.JsonParser().parse(s).getAsJsonObject();
                    return RowFactory.create(obj.get("url").getAsString(), obj.get("ip").getAsString(),
                            obj.get("clientKey").getAsString());
                }, Encoders.row(clickSchema))
                .repartition(parserPar)
                .observe("parser", functions.count(functions.lit(1)).cast("double").as("rows"));

        Dataset<Row> formatted;
        int sinkPar;

        if (query == Constants.QUERY_ONE || query == Constants.QUERY_THREE) {
            int repeatVisitPar = Integer.parseInt(pd[2]);
            int reducePar = Integer.parseInt(pd[3]);
            sinkPar = Integer.parseInt(pd[4]);
            spark.conf().set("spark.sql.shuffle.partitions", repeatVisitPar);

            // Stage [2]: repeat-visit -- total visits and unique clientKeys per URL per window.
            String windowDuration = slidingWindowSize + " milliseconds";
            String slideDuration = slidingWindowSlide + " milliseconds";
            Dataset<Row> repeatVisit = parsed
                    .withColumn("event_time", functions.current_timestamp())
                    .groupBy(
                            functions.window(functions.col("event_time"), windowDuration, slideDuration),
                            functions.col("url"))
                    .agg(functions.count("*").as("totalVisits"),
                            functions.approx_count_distinct(functions.col("clientKey")).as("uniqueVisits"))
                    .observe("repeatvisit", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [3]: reduce-operation (folded into the aggregation above -- see class javadoc).
            Dataset<Row> reduced = repeatVisit.repartition(reducePar)
                    .observe("reduce", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [4]: sink, mirrors Tuple3.toString() = "(url,totalVisits,uniqueVisits)".
            formatted = reduced
                    .selectExpr("CAST(concat('(', url, ',', CAST(totalVisits AS STRING), ',', " +
                            "CAST(uniqueVisits AS STRING), ')') AS STRING) AS value")
                    .repartition(sinkPar)
                    .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));

        } else if (query == Constants.QUERY_TWO) {
            int geoPar = Integer.parseInt(pd[2]);
            sinkPar = Integer.parseInt(pd[3]);
            spark.conf().set("spark.sql.shuffle.partitions", geoPar);

            // Stage [2]: geography-visit.
            StructType geoSchema = new StructType().add("country", "string").add("city", "string");
            Dataset<Row> geo = parsed.mapPartitions(
                    (MapPartitionsFunction<Row, Row>) it -> {
                        DatabaseReader reader;
                        try {
                            URL databaseURL = new URL("https://git.io/GeoLite2-City.mmdb");
                            reader = new DatabaseReader.Builder(databaseURL.openStream()).build();
                        } catch (Exception e) {
                            reader = null;
                        }
                        List<Row> out = new ArrayList<>();
                        while (it.hasNext()) {
                            Row r = it.next();
                            String ip = r.getString(1);
                            try {
                                if (reader == null) {
                                    throw new AddressNotFoundException("no database");
                                }
                                CityResponse response = reader.city(InetAddress.getByName(ip));
                                out.add(RowFactory.create(response.getCountry().getName(), response.getCity().getName()));
                            } catch (AddressNotFoundException e) {
                                out.add(RowFactory.create("India", "Bangalore"));
                            } catch (Exception e) {
                                // matches the Flink source having no catch-all here, but a
                                // malformed/unreachable lookup should not kill the query
                                out.add(RowFactory.create("India", "Bangalore"));
                            }
                        }
                        return out.iterator();
                    }, Encoders.row(geoSchema))
                    .repartition(geoPar)
                    .observe("geo", functions.count(functions.lit(1)).cast("double").as("rows"));

            // Stage [3]: sink, mirrors GeoStats.toString().
            formatted = geo
                    .selectExpr("CAST(concat('GeoStats{country=\\'', country, '\\', cityVisits={', " +
                            "city, '=1}}') AS STRING) AS value")
                    .repartition(sinkPar)
                    .observe("sink", functions.count(functions.lit(1)).cast("double").as("rows"));
        } else {
            throw new IllegalArgumentException("The only supported queries are \"1\", \"2\", and \"3\".");
        }

        String checkpointLocation = "/tmp/spark-checkpoints/clickanalytics-" + System.currentTimeMillis();

        StreamingQuery streamingQuery = formatted.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaServer)
                .option("topic", output)
                .option("checkpointLocation", checkpointLocation)
                .outputMode("update")
                .start();

        streamingQuery.awaitTermination(secondsToWait * 1000);
        streamingQuery.stop();
    }
}
