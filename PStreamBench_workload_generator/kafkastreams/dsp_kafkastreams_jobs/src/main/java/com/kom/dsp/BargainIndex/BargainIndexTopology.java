package com.kom.dsp.BargainIndex;

import com.kom.dsp.utils.ArgParser;
import com.kom.dsp.utils.Constants;
import com.kom.dsp.utils.OperatorMetricsProcessor;
import com.kom.dsp.utils.WindowConfig;
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

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams counterpart of
 * flink/dsp_flink_jobs/.../BargainIndex/BargainIndex.java and
 * spark/dsp_spark_jobs/.../BargainIndex/BargainIndex.java.
 *
 * QuoteDataParser.java parses 8 fields (symbol, date, open, high, low,
 * close, adjClose, volume) but only symbol/close/volume are ever read
 * downstream, so only those are carried forward -- same simplification the
 * Spark port already made. The windowed VWAP aggregate (sum(close*volume),
 * sum(volume), last close) has no built-in Kafka Streams aggregator, so it
 * is accumulated as a delimited "sumPriceVolume,sumVolume,lastClose"
 * string via a custom Aggregator, avoiding a custom POJO + Serde -- same
 * "plain delimited strings, no custom serialization" style already used
 * for the CLI/topic wire formats elsewhere in this project. "lastClose" is
 * whichever record's close price is aggregated last within the window, an
 * approximation Kafka Streams shares with the Spark port (Structured
 * Streaming does not guarantee micro-batch arrival order either).
 *
 * Stage shape: [0] source, [1] parser, [2] vwap, [3] bargain, [4] sink --
 * same as Flink/Spark.
 */
public class BargainIndexTopology {

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
        int popularityThreshold = Integer.parseInt(params.get("popularityThreshold"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        long graceMsArg = Long.parseLong(params.get("graceMs", "-1"));

        if (!mode.equalsIgnoreCase(Constants.KAFKA)) {
            throw new IllegalArgumentException(
                    "Kafka Streams Bargain Index job currently only supports mode=KAFKA");
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

        // Stage [parser]: mirrors QuoteDataParser.java's field layout
        // (symbol=fields[0], close=fields[5], volume=fields[7]); re-keyed by
        // symbol, value carried forward as "close,volume".
        KStream<String, String> parsed = source
                .map((key, line) -> {
                    String[] fields = line.split(",");
                    String symbol = fields[0];
                    double close = Double.parseDouble(fields[5]);
                    long volume = Long.parseLong(fields[7]);
                    return new KeyValue<>(symbol, close + "," + volume);
                })
                .process(OperatorMetricsProcessor.supplier("parser"));

        // Stage [vwap]: hopping-window aggregate per symbol -- accumulator
        // string is "sumPriceVolume,sumVolume,lastClose".
        KTable<Windowed<String>, String> aggregated = parsed
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(WindowConfig.build(slidingWindowSize, slidingWindowSlide,
                        WindowConfig.Unit.MILLIS, graceMsArg, "BargainIndex"))
                .aggregate(
                        () -> "0.0,0,0.0",
                        (symbol, value, agg) -> {
                            String[] quote = value.split(",");
                            double close = Double.parseDouble(quote[0]);
                            long volume = Long.parseLong(quote[1]);
                            String[] accParts = agg.split(",");
                            double sumPriceVolume = Double.parseDouble(accParts[0]) + close * volume;
                            long sumVolume = Long.parseLong(accParts[1]) + volume;
                            return sumPriceVolume + "," + sumVolume + "," + close;
                        },
                        Materialized.with(Serdes.String(), Serdes.String()));

        // vwap = sumPriceVolume / sumVolume, totalVolume = sumVolume,
        // quotePrice = lastClose -- re-encoded as "vwap,totalVolume,quotePrice"
        // for the bargain stage.
        KStream<String, String> vwap = aggregated.toStream()
                .process(OperatorMetricsProcessor.supplier("vwap"))
                .map((windowedKey, acc) -> {
                    String[] parts = acc.split(",");
                    double sumPriceVolume = Double.parseDouble(parts[0]);
                    double sumVolume = Double.parseDouble(parts[1]);
                    double lastClose = Double.parseDouble(parts[2]);
                    double vwapValue = sumPriceVolume / sumVolume;
                    return new KeyValue<>(windowedKey.key(), vwapValue + "," + sumVolume + "," + lastClose);
                });

        // Stage [bargain]: bargainIndex = quotePrice > vwap ? quotePrice/vwap : 0.0,
        // filtered by popularityThreshold, formatted as
        // "symbol,bargainIndex,totalVolume" (mirrors BargainSerializationSchema.java).
        KStream<String, String> bargains = vwap
                .mapValues((symbol, value) -> {
                    String[] parts = value.split(",");
                    double vwapValue = Double.parseDouble(parts[0]);
                    double totalVolume = Double.parseDouble(parts[1]);
                    double quotePrice = Double.parseDouble(parts[2]);
                    double bargainIndex = (quotePrice > vwapValue) ? (quotePrice / vwapValue) : 0.0;
                    return bargainIndex + "," + totalVolume;
                })
                .filter((symbol, value) -> Double.parseDouble(value.split(",")[0]) > popularityThreshold)
                .map((symbol, value) -> {
                    String[] parts = value.split(",");
                    return new KeyValue<>(symbol, symbol + "," + parts[0] + "," + parts[1]);
                })
                .process(OperatorMetricsProcessor.supplier("bargain"));

        // Stage [sink].
        bargains
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
