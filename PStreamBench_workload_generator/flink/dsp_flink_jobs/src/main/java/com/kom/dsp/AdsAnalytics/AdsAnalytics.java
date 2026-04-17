package com.kom.dsp.AdsAnalytics;

import com.kom.dsp.utils.Constants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class AdsAnalytics {
    //private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        // create the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(10);

        System.out.println("[main] Execution environment created.");

        ParameterTool params = ParameterTool.fromArgs(args);

        //int parallelism = Integer.parseInt(params.get("parallelism"));
        String parallelism = params.get("parallelism").replace(" ","").replace("[","").replace("]","").replace("'","");
        String[] parallelism_degree = parallelism.split(",");
        int query = Integer.parseInt(params.get("query"));
        String mode = params.get("mode");
        String input = params.get("input"); System.out.println(input);
        String output = params.get("output"); System.out.println(output);
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        String bootstrapServer;

        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        int watermarkLateness = Integer.parseInt(params.get("lateness"));
        int topicPopularityThreshold = Integer.parseInt(params.get("popularityThreshold"));

        env.setParallelism(Integer.parseInt(parallelism_degree[0]));

        DataStream<String> clicks;
        DataStream<String> impressions;
        KafkaSink<?> kafkaSink = null;
        FileSink<?> fileSink = null;
        if (mode.equalsIgnoreCase(Constants.FILE)) {
            System.out.println("[main] Arguments parsed.");
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineFormat(), new Path(input))
                    .monitorContinuously(Duration.ofSeconds(10))
                    .build();
            clicks = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input-clicks");
            impressions = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input-impressions");

             fileSink = FileSink.<String>forRowFormat(
                            new Path(output), new SimpleStringEncoder<>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withMaxPartSize(20000L)
                                    .withRolloverInterval(1000L)
                                    .build())
                    .withBucketAssigner(new BasePathBucketAssigner<>())
                    .build();

        } else {
            if (mode.equalsIgnoreCase(Constants.KAFKA)) {
                bootstrapServer = params.get("kafka-server");
                System.out.println("[main] Arguments parsed.");
                KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServer)
                        //.setProperty("partition.discovery.interval.ms", "10000")
                        //.setStartingOffsets(OffsetsInitializer.earliest())
                        //.setTopicPattern(input+".*")
                        .setTopics(input)
                        .setGroupId("my-group")
                        .setProperty("fetch.min.bytes", "100000")
                        .setProperty("fetch.max.wait.ms", "50")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .build();

                clicks = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "kafka-source")
                        .setParallelism(Integer.parseInt(parallelism_degree[0]));
                impressions = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "kafka-source-impressions")
                        .setParallelism(Integer.parseInt(parallelism_degree[0]));

                kafkaSink = KafkaSink.<RollingCTR>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                .setValueSerializationSchema((SerializationSchema<RollingCTR>) element -> element.toString().getBytes())
                                .build()
                        )
                        .setProperty("batch.size", "200000")
                        .setProperty("batch.num.messages", "200000")
                        .setProperty("linger.ms", "10")
                        .setProperty("compression.type", "lz4")
                        .setProperty("acks", "1")
                        .build();

            } else {
                throw new IllegalArgumentException("The only supported modes are \"file\" and \"kafka\".");
            }
        }

        System.out.println("[main] Source and Sink created.");

        if (query == Constants.QUERY_ONE) {

            //query1 start
            //the parallelism from frontend will contain 5 parallelism degrees i.e click-parser,
            //impression-parser, clicks-counter, impressions-counter, rollingCTR
            DataStream<AdEvent> parsedClicks =
                    clicks.flatMap(new Q1ClickParser())
                            .setParallelism(Integer.parseInt(parallelism_degree[1]))
                            .name("click-parser");

            DataStream<AdEvent> parsedImpressions =
                    impressions.flatMap(new Q1ImpressionParser())
                            .setParallelism(Integer.parseInt(parallelism_degree[2]))
                            .name("impression-parser");

            System.out.println("[main] Parsers created.");

            DataStream<AdEvent> clicksCounter = parsedClicks
                    .keyBy(new KeySelector<AdEvent, Tuple2<Long, Long>>() {
                               @Override
                               public Tuple2<Long, Long> getKey(AdEvent value) throws Exception {
                                   return Tuple2.of(value.getQueryId(), value.getAdId());
                               }
                           }
                    )
                    .sum("count")
                    .setParallelism(Integer.parseInt(parallelism_degree[3]))
                    .name("clicks-counter");

            System.out.println("[main] ClicksCounter created.");

            DataStream<AdEvent> impressionsCounter = parsedImpressions
                    .keyBy(new KeySelector<AdEvent, Tuple2<Long, Long>>() {
                               @Override
                               public Tuple2<Long, Long> getKey(AdEvent value) throws Exception {
                                   return Tuple2.of(value.getQueryId(), value.getAdId());
                               }
                           }
                    )
                    .sum("count")
                    .setParallelism(Integer.parseInt(parallelism_degree[4]))
                    .name("impressions-counter");;

            System.out.println("[main] ImpressionsCounter created.");


            //TODO: parametrize sliding window slide and size & add parallelism 
            // with instead of apply to set setParallelism(parallelism).
            DataStream<RollingCTR> rollingCTR = clicksCounter
                    .join(impressionsCounter)
                    .where(new KeySelector<AdEvent, Tuple2<Long, Long>>() {
                        @Override
                        public Tuple2<Long, Long> getKey(AdEvent value) throws Exception {
                            return Tuple2.of(value.getQueryId(), value.getAdId());
                        }
                    }).equalTo(new KeySelector<AdEvent, Tuple2<Long, Long>>() {
                        @Override
                        public Tuple2<Long, Long> getKey(AdEvent value) throws Exception {
                            return Tuple2.of(value.getQueryId(), value.getAdId());
                        }
                    })
                    .window(SlidingProcessingTimeWindows.of(
                                Time.milliseconds(slidingWindowSize),
                                Time.milliseconds(slidingWindowSlide)))
                    .with(new JoinFunction<AdEvent, AdEvent, RollingCTR>() {
                        @Override
                        public RollingCTR join(AdEvent first, AdEvent second) throws Exception {
                            return new RollingCTR(first.getQueryId(), first.getAdId(), first.getCount(), second.getCount(), first.getCount() / (float) second.getCount());
                        }
                    })
                    .setParallelism(Integer.parseInt(parallelism_degree[5]))
                    .name("join")
                    .filter(v -> v.getClicks() <= v.getImpressions())
                    .setParallelism(Integer.parseInt(parallelism_degree[5]))
                    .name("rollingCTR");
            //end query1
            System.out.println("[main] RollingCTR created.");

            if (mode.equalsIgnoreCase(Constants.FILE)) {
                rollingCTR.sinkTo((Sink<RollingCTR>) fileSink).name("file-sink");
            }
            if (mode.equalsIgnoreCase(Constants.KAFKA)) {
                //processedOutputQ1.sinkTo((Sink<ProcessOutputQ1>) kafkaSink).name("kafka-sink");
                rollingCTR.sinkTo((Sink<RollingCTR>) kafkaSink).setParallelism(Integer.parseInt(parallelism_degree[6])).name("kafka-sink");
                //rollingCTR.sinkTo(sink).name("kafka-sink");
            }

            env.disableOperatorChaining();
            System.out.println("[main] RollingCTR sinks.");

        } else if (query == Constants.QUERY_TWO) {

            DataStream<AdEvent> parsedAdEvents =
                clicks.flatMap(new Q2AdEventParser())
                    .setParallelism(Integer.parseInt(parallelism_degree[1]))
                    .name("parser");

            DataStream<RollingCTR> counter = parsedAdEvents
                .flatMap(new Q2AdEventCounter())
                .setParallelism(Integer.parseInt(parallelism_degree[2]))
                .name("counter");

            DataStream<RollingCTR> rollingCTR = counter
                .keyBy(new KeySelector<RollingCTR, Tuple2<Long, Long>>() {
                           @Override
                           public Tuple2<Long, Long> getKey(RollingCTR value) throws Exception {
                               return Tuple2.of(value.getQueryId(), value.getAdId());
                           }
                       }
                )
                .window(SlidingProcessingTimeWindows.of(
                            Time.milliseconds(slidingWindowSize),
                            Time.milliseconds(slidingWindowSlide)))
                .process(new Q2RollingAverage())
                .setParallelism(Integer.parseInt(parallelism_degree[3]))
                .name("rollingCTR");

            if (mode.equalsIgnoreCase(Constants.FILE)) {
                rollingCTR.sinkTo((Sink<RollingCTR>) fileSink).name("file-sink");
            }
            if (mode.equalsIgnoreCase(Constants.KAFKA)) {
                rollingCTR.sinkTo((Sink<RollingCTR>) kafkaSink).setParallelism(Integer.parseInt(parallelism_degree[4])).name("kafka-sink");
            }

            env.disableOperatorChaining();
            System.out.println("[main] RollingCTR sinks.");
        }
        //env.execute("Ads Analytics");
        JobClient client = env.executeAsync("Ads Analytics");
        System.out.println("Time to cancel activate execution");
        long start = System.currentTimeMillis();
        long end = start + secondsToWait * 1000;
        while (System.currentTimeMillis() < end) {
            // Some expensive operation on the item.
        }
        CompletableFuture<Void> future=client.cancel();

        System.out.println("Job should be cancelled "+future.isDone());

    }
}
