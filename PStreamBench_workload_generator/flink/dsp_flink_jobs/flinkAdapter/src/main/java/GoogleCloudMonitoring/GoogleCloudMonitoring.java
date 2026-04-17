package GoogleCloudMonitoring;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

public class GoogleCloudMonitoring {
    //private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        // create the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //env.getConfig().setAutoWatermarkInterval(1000);
        System.out.println("[main] Execution environment created.");

       ParameterTool params = ParameterTool.fromArgs(args);

        int parallelism = Integer.parseInt(params.get("parallelism"));

        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String bootstrapServer;
        int query = Integer.parseInt(params.get("query"));

        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));

        int watermarkLateness = Integer.parseInt(params.get("lateness"));
        
        /*int parallelism = 4;

        String mode = "kafka";
        String input = "googleMonitoring-input";
        String output = "googleMonitoring-output";
        String bootstrapServer = "localhost:9092";
        int query =1;

        int slidingWindowSize = 5;
        int slidingWindowSlide = 1;

        int watermarkLateness = 10;*/
        
        
        DataStream<String> source;
        Sink sink;
        if (mode.equals("file")) {
            System.out.println("[main] Arguments parsed.");
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineFormat(), new Path(input))
                    .monitorContinuously(Duration.ofSeconds(10))
                    .build();
            source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input");

            FileSink<CPUPerCatgory> fileSinkQuery1 = FileSink.<CPUPerCatgory>forRowFormat(
                            new Path(output), new SimpleStringEncoder<>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withMaxPartSize(20000L)
                                    .withRolloverInterval(1000L)
                                    .build())
                    .withBucketAssigner(new BasePathBucketAssigner<>())
                    .build();

            FileSink<CPUPerJob> fileSinkQuery2 = FileSink.<CPUPerJob>forRowFormat(
                            new Path(output), new SimpleStringEncoder<>())
                    .withRollingPolicy(
                            DefaultRollingPolicy.builder()
                                    .withMaxPartSize(20000L)
                                    .withRolloverInterval(1000L)
                                    .build())
                    .withBucketAssigner(new BasePathBucketAssigner<>())
                    .build();

            if (query == 1) {
                sink = fileSinkQuery1;
            } else {
                if (query == 2) {
                    sink = fileSinkQuery2;
                } else {
                    throw new IllegalArgumentException("The only supported queries are \"1\" and \"2\".");
                }
            }
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
                source = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source");

                KafkaSink<CPUPerCatgory> kafkaSinkQuery1 = KafkaSink.<CPUPerCatgory>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                .setValueSerializationSchema((SerializationSchema<CPUPerCatgory>) element -> element.toString().getBytes())
                                .build()
                        )
                        .build();

                KafkaSink<CPUPerJob> kafkaSinkQuery2 = KafkaSink.<CPUPerJob>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                .setValueSerializationSchema((SerializationSchema<CPUPerJob>) element -> element.toString().getBytes())
                                .build()
                        )
                        .build();

                if (query == 1) {
                    sink = kafkaSinkQuery1;
                } else {
                    if (query == 2) {
                        sink = kafkaSinkQuery2;
                    } else {
                        throw new IllegalArgumentException("The only supported queries are \"1\" and \"2\".");
                    }
                }
            } else {
                throw new IllegalArgumentException("The only supported modes are \"file\" and \"kafka\".");
            }
        }

        System.out.println("[main] Source and Sink created.");
        /*source.map(new MapFunction<String,String>() {

			@Override
			public String map(String value) throws Exception {
				System.out.println(value);
				return null;
			}
        	
        });*/
       // DataStream<TaskEvent>  parser = source.map(new TaskEventParser());
        
        
        DataStream<TaskEvent> parsedEntries =
                source.map(new TaskEventParser()).name("parser")
                        .setParallelism(parallelism)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<TaskEvent>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkLateness)));
                                //.withTimestampAssigner((event, timestamp) -> (long) (1416304680000L + event.getTimestamp() * 0.001))); //*1000 if in seconds

        System.out.println("[main] Parser created.");
        /*parsedEntries.map(new MapFunction<TaskEvent,TaskEvent>() {

			@Override
			public TaskEvent map(TaskEvent value) throws Exception {
				System.out.println(value);
				return null;
			}
        	
        });*/

        if (query == 1) {
            DataStream<CPUPerCatgory> averageCPUPerCategory = parsedEntries
                    .keyBy(value -> value.getCategory())
                    .window(SlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.seconds(slidingWindowSlide)))
                    .apply(new CPUPerCategoryCalculator())
                    .setParallelism(parallelism)
                    .name("average-cpu-per-category");

            System.out.println("[main] AverageCPUPerCategoryCalculator [Query 1] created.");

            if (mode.equals("file")) {
                averageCPUPerCategory.sinkTo(sink).name("file-sink");
            }
            if (mode.equals("kafka")) {
                averageCPUPerCategory.sinkTo(sink).name("kafka-sink");
            }
            /*averageCPUPerCategory.map(new MapFunction<CPUPerCatgory,CPUPerCatgory>() {

    			@Override
    			public CPUPerCatgory map(CPUPerCatgory value) throws Exception {
    				System.out.println(value);
    				return null;
    			}
            	
            });*/
            System.out.println("[main] AverageCPUPerCategoryCalculator [Query 1] sinks.");

        } else {
            if (query == 2) {
                DataStream<CPUPerJob> averageCPUPerJob = parsedEntries
                        .filter(value -> value.getEventType() == 1)
                        .keyBy(value -> value.getJobId())
                        .window(SlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.seconds(slidingWindowSlide)))
                        .apply(new CPUPerJobCalculator())
                        .setParallelism(parallelism)
                        .name("average-cpu-per-job");

                System.out.println("[main] AverageCPUPerJobCalculator [Query 2] created.");

                if (mode.equals("file")) {
                    averageCPUPerJob.sinkTo(sink).name("file-sink");
                }
                if (mode.equals("kafka")) {
                    averageCPUPerJob.sinkTo(sink).name("kafka-sink");
                }

                System.out.println("[main] AverageCPUPerJobCalculator [Query 2] sinks.");

            }
        }
        env.disableOperatorChaining();
        env.execute("Google Cloud Monitoring");
    }
}