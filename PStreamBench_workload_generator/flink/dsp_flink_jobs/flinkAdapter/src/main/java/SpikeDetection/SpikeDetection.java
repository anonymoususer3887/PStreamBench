package SpikeDetection;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class SpikeDetection {
    //private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      // env.getConfig().setAutoWatermarkInterval(1000);
       env.getConfig().setLatencyTrackingInterval(10);
        System.out.println("[main] Execution environment created.");
        System.out.println(args);
        ParameterTool params = ParameterTool.fromArgs(args);

       int parallelism = Integer.parseInt(params.get("parallelism"));System.out.println(parallelism);
        String mode = params.get("mode"); System.out.println(mode);
        String input = params.get("input");System.out.println(input);
        String output = params.get("output"); System.out.println(output);
        String bootstrapServer;

        int slidingWindowSize = Integer.parseInt(params.get("size")); System.out.println(slidingWindowSize);
        int slidingWindowSlide = Integer.parseInt(params.get("slide")); System.out.println(slidingWindowSlide);

        int watermarkLateness = Integer.parseInt(params.get("lateness")); System.out.println(watermarkLateness);
        
        DataStream<String> source;
        KafkaSink<AverageValue> kafkaSink = null;
        FileSink<AverageValue> fileSink = null;
        Sink sink;
        if (mode.equals("file")) {
            System.out.println("[main] Arguments parsed.");
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineFormat(), new Path(input))
                    .monitorContinuously(Duration.ofSeconds(10))
                    .build();
            source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input");

            sink = FileSink.<AverageValue>forRowFormat(
                            new Path(output), new SimpleStringEncoder<>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withMaxPartSize(20000L)
                                    .withRolloverInterval(1000L)
                                    .build())
                    .withBucketAssigner(new BasePathBucketAssigner<>())
                    .build();
        } else {
            if (mode.equals("kafka")) {
                bootstrapServer = params.get("kafka-server"); System.out.println(bootstrapServer);
                System.out.println("[main] Arguments parsed.");
                KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setTopics(input)
                        .setGroupId("my-group")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .build();
                source = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source");

                sink = KafkaSink.<AverageValue>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                .setValueSerializationSchema((SerializationSchema<AverageValue>) element -> element.toString().getBytes())
                                .build()
                        )
                        .build();
            } else {
                throw new IllegalArgumentException("The only supported modes are \"file\" and \"kafka\".");
            }
        }

        System.out.println("[main] Source and Sink created.");

        DataStream<SensorMeasurement> parser =
                source.flatMap(new SensorParser()).setParallelism(parallelism)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorMeasurement>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkLateness)) )  
                            // .withTimestampAssigner((event, timestamp) -> event.getTimestamp()))
                        .name("parser");

        System.out.println("[main] SensorParser created.");

        DataStream<AverageValue> averageCalculator =
                parser.keyBy(value ->(""+ value.getSensorId() ))
                       //.window(SlidingEventTimeWindows.of(Time.seconds(slidingWindowSize), Time.milliseconds(slidingWindowSlide*10))) //.countWindow(3,1)
                		.window(SlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.seconds(slidingWindowSlide)))
                        .process(new AverageValueCalculator())
                        .setParallelism(parallelism)
                        .name("average-calculator");

        System.out.println("[main] AverageValueCalculator created.");

        DataStream<AverageValue> spikeDetector =
                averageCalculator
                        .filter(value -> Math.abs(value.currentValue - value.averageValue) > 0.03d * value.averageValue)
                		//.filter(value -> 6>1)
                        .setParallelism(parallelism)
                        .name("spike-detector");


        System.out.println("[main] SpikeDetector created.");

        if (mode.equals("file")) {
        	spikeDetector.sinkTo(sink).name("file-sink");
        }
        if (mode.equals("kafka")) {
        	spikeDetector.sinkTo(sink).name("kafka-sink");
        }

        System.out.println("[main] SpikeDetector sinks.");
        env.disableOperatorChaining();
        env.execute("Spike Detection");
        System.out.println("after execution");
        

    }
}
