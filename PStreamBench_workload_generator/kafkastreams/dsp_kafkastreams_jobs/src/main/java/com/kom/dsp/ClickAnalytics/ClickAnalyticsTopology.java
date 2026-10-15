package com.kom.dsp.ClickAnalytics;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.OperatorMetricsProcessor;
import com.kom.dsp.utils.WindowConfig;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../ClickAnalytics/ClickAnalytics.java and
 * spark/dsp_spark_jobs/.../ClickAnalytics/ClickAnalytics.java.
 *
 * Query 1/3: total visits and *distinct* clientKeys per URL per window,
 * tracked directly (an exact per-window Set<String>, not Spark's
 * approx_count_distinct -- Kafka Streams has no built-in approx-distinct
 * aggregator either, and windows here are small enough that an exact set
 * is cheap), same "cleaner in-window-scoped reading" the Spark port already
 * chose over Flink's HashMap-scoped-to-the-operator-instance original. The
 * Flink source's separate "reduce" stage then sums that result again with a
 * running total; folded into the same windowed aggregate here too (no
 * second streaming aggregation on top of a first), same as the Spark port
 * -- "reduce" stage below is a passthrough marker only, to keep the same
 * stage/operator_names shape as every other engine's collector.
 * Query 3 uses a Flink count-window; there is no Kafka Streams count-window
 * primitive either (same gap Spark has), so this reuses query 1's
 * time-based window.
 *
 * Query 2's GeoIP2 lookup (com.maxmind.geoip2) is loaded once per JVM from
 * a copy of the .mmdb bundled into this jar's resources (see
 * src/main/resources/GeoLite2-City.mmdb) -- same reasoning as the Spark
 * port: a live per-batch HTTP fetch is not safe under a re-planned/re-run
 * execution model, and Kafka Streams' one-JVM-per-instance model means a
 * static singleton is even cheaper here than Spark's per-partition load.
 *
 * Stage shape: [0] source, [1] parser, [2] repeatvisit, [3] reduce,
 * [4] sink (queries 1/3) or [0] source, [1] parser, [2] geo, [3] sink
 * (query 2) -- same as Flink/Spark.
 */
public class ClickAnalyticsTopology {

    private static final DatabaseReader GEO_READER = loadGeoReader();

    private static DatabaseReader loadGeoReader() {
        try {
            return new DatabaseReader.Builder(
                    ClickAnalyticsTopology.class.getResourceAsStream("/GeoLite2-City.mmdb")).build();
        } catch (Exception e) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        ArgParser params = ArgParser.fromArgs(args);

        String parallelism = params.get("parallelism")
                .replace(" ", "").replace("[", "").replace("]", "").replace("'", "");
        int streamThreads = Integer.parseInt(parallelism.split(",")[1]); // operator (swept) parallelism, matching Flink/Storm/Spark sweep vector shape

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String kafkaServer = params.get("kafka-server");
        String applicationId = params.get("application-id");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int query = Integer.parseInt(params.get("query"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        long graceMsArg = Long.parseLong(params.get("graceMs", "-1"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams Click Analytics job currently only supports mode=KAFKA");
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        StreamsBuilder builder = new StreamsBuilder();

        // Stage [source].
        KStream<String, String> source = builder
                .stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                .process(OperatorMetricsProcessor.supplier("source"));

        // Stage [parser]: mirrors ClickLogParser.java (JSON fields
        // url/ip/clientKey), carried forward as "url,ip,clientKey".
        KStream<String, String> parsed = source
                .map((key, json) -> {
                    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
                    String url = obj.get("url").getAsString();
                    String ip = obj.get("ip").getAsString();
                    String clientKey = obj.get("clientKey").getAsString();
                    return new KeyValue<String, String>(null, url + "," + ip + "," + clientKey);
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        KStream<String, String> formatted;

        if (query == Constants.QUERY_TWO) {
            // Stage [geo]: GeoIP2 city/country lookup, falling back to the
            // Flink source's own "India, Bangalore" catch-all on any lookup
            // failure (unresolvable IP, missing/corrupt database, ...).
            KStream<String, String> geo = parsed
                    .mapValues(value -> {
                        String ip = value.split(",")[1];
                        try {
                            if (GEO_READER == null) {
                                throw new AddressNotFoundException("no database");
                            }
                            CityResponse response = GEO_READER.city(InetAddress.getByName(ip));
                            return response.getCountry().getName() + "," + response.getCity().getName();
                        } catch (Exception e) {
                            return "India,Bangalore";
                        }
                    })
                    .process(OperatorMetricsProcessor.supplier("geo"));

            // Stage [sink], mirrors GeoStats.toString().
            formatted = geo.mapValues(value -> {
                String[] parts = value.split(",");
                String country = parts[0];
                String city = parts[1];
                return "GeoStats{country='" + country + "', cityVisits={" + city + "=1}}";
            });
        } else {
            // Stage [repeatvisit]: total visits + distinct clientKeys per
            // URL per window -- accumulator is "totalVisits;key1,key2,...".
            KStream<String, String> byUrl = parsed
                    .map((key, value) -> {
                        String[] parts = value.split(",");
                        return new KeyValue<>(parts[0], parts[2]);
                    });

            KTable<Windowed<String>, String> aggregated = byUrl
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                            WindowConfig.Unit.MILLIS, graceMsArg, "ClickAnalytics"))
                    .aggregate(
                            () -> "0;",
                            (url, clientKey, agg) -> {
                                String[] accParts = agg.split(";", 2);
                                long totalVisits = Long.parseLong(accParts[0]) + 1;
                                String keysStr = (accParts.length > 1) ? accParts[1] : "";
                                Set<String> keys = keysStr.isEmpty()
                                        ? new HashSet<>()
                                        : new HashSet<>(Arrays.asList(keysStr.split(",")));
                                keys.add(clientKey);
                                return totalVisits + ";" + String.join(",", keys);
                            },
                            Materialized.with(Serdes.String(), Serdes.String()));

            KStream<Windowed<String>, String> repeatVisit = aggregated.toStream()
                    .process(OperatorMetricsProcessor.supplier("repeatvisit"));

            // Stage [reduce]: passthrough marker only -- see class javadoc
            // for why there is no second aggregation here.
            KStream<Windowed<String>, String> reduced = repeatVisit
                    .process(OperatorMetricsProcessor.supplier("reduce"));

            // Stage [sink], mirrors Tuple3.toString() = "(url,totalVisits,uniqueVisits)".
            formatted = reduced.map((windowedKey, acc) -> {
                String[] accParts = acc.split(";", 2);
                long totalVisits = Long.parseLong(accParts[0]);
                String keysStr = (accParts.length > 1) ? accParts[1] : "";
                long uniqueVisits = keysStr.isEmpty() ? 0 : keysStr.split(",").length;
                String line = "(" + windowedKey.key() + "," + totalVisits + "," + uniqueVisits + ")";
                return new KeyValue<>(windowedKey.key(), line);
            });
        }

        formatted
                .process(OperatorMetricsProcessor.supplier("sink"))
                .to(output, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            streams.cleanUp();
            latch.countDown();
        }));

        System.out.println("[main] Starting Kafka Streams, application.id=" + applicationId);
        streams.start();

        System.out.println("[main] Will run for " + secondsToWait + " seconds.");
        Thread.sleep(secondsToWait * 1000);
        streams.close();
        // Wipes this run's local RocksDB state store (/tmp/kafka-streams/<application.id>/) -- application.id is uniquely
        // timestamped per submission (see runner.py's _run_kafkastreams_job()),
        // so nothing is ever restarted/recovered from this state; leaving it on disk
        // is pure leak. Confirmed live (2026-09-24): unremoved state from thousands of
        // prior submissions across this project's strategy sweep filled every node's
        // root filesystem to 100%, silently failing every subsequent job (Ansible
        // couldn't even create its own tmp dir) regardless of workload or parallelism.
        streams.cleanUp();
        System.out.println("[main] Streams closed.");
    }
}
