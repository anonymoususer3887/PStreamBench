package AdsAnalytics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink.Sink;
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
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class AdsAnalytics {
    //private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        // create the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(10);
        System.out.println("[main] Execution environment created.");

        ParameterTool params = ParameterTool.fromArgs(args);

        int parallelism = Integer.parseInt(params.get("parallelism"));
        String mode = params.get("mode");
        String input = params.get("input"); System.out.println(input);
        String output = params.get("output"); System.out.println(output);
        String bootstrapServer;

        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        

        DataStream<String> clicks;
        DataStream<String> impressions;
        Sink sink;
        if (mode.equals("file")) {
            System.out.println("[main] Arguments parsed.");
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineFormat(), new Path(input))
                    .monitorContinuously(Duration.ofSeconds(10))
                    .build();
            clicks = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input-clicks");
            impressions = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input-impressions");

            FileSink<String> fileSink = FileSink.<String>forRowFormat(
                            new Path(output), new SimpleStringEncoder<>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withMaxPartSize(20000L)
                                    .withRolloverInterval(1000L)
                                    .build())
                    .withBucketAssigner(new BasePathBucketAssigner<>())
                    .build();
            sink = fileSink;
        } else {
            if (mode.equals("kafka")) {
                bootstrapServer = params.get("kafka-server");
                System.out.println("[main] Arguments parsed.");
                KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setTopics(input)
                        .setGroupId("my-group")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .build();
                clicks = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "kafka-source-clicks");
                impressions = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "kafka-source-impressions");

                KafkaSink<RollingCTR> kafkaSink = KafkaSink.<RollingCTR>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                .setValueSerializationSchema((SerializationSchema<RollingCTR>) element -> element.toString().getBytes())
                                .build()
                        )
                        .build();
                sink = kafkaSink;
            } else {
                throw new IllegalArgumentException("The only supported modes are \"file\" and \"kafka\".");
            }
        }

        System.out.println("[main] Source and Sink created.");

        DataStream<AdEvent> parsedClicks =
                clicks.flatMap(new ClickParser())
                        .setParallelism(parallelism)
                        .name("click-parser");

        DataStream<AdEvent> parsedImpressions =
                impressions.flatMap(new ImpressionParser())
                        .setParallelism(parallelism)
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
                .setParallelism(parallelism)
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
                .setParallelism(parallelism)
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
                .window(SlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.seconds(slidingWindowSlide)))
                .with(new JoinFunction<AdEvent, AdEvent, RollingCTR>() {
                    @Override
                    public RollingCTR join(AdEvent first, AdEvent second) throws Exception {
                        return new RollingCTR(first.getQueryId(), first.getAdId(), first.getCount(), second.getCount(), first.getCount() / second.getCount());
                    }
                }).setParallelism(parallelism).filter(value -> value.getClicks()<= value.getImpressions()).setParallelism(parallelism).name("rollingCTR");

        System.out.println("[main] RollingCTR created.");

        if (mode.equals("file")) {
            rollingCTR.sinkTo(sink).name("file-sink");
        }
        if (mode.equals("kafka")) {
            rollingCTR.sinkTo(sink).name("kafka-sink");
        }
        env.disableOperatorChaining();
        System.out.println("[main] RollingCTR sinks.");
        env.execute("Ads Analytics");
    }
}