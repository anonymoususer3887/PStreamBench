package SmartGrid;

import java.time.Duration;
import java.util.ArrayList;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink.Sink;
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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.api.java.utils.ParameterTool;



import SmartGrid.AverageValue;
import SmartGrid.AverageValueCalculator;
import SmartGrid.GridEvent;
import SmartGrid.ValueParser;

public class SmartGrid {
	//private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);
  
	public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //env.getConfig().setAutoWatermarkInterval(1000);
        env.getConfig().setLatencyTrackingInterval(10);
        
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
        

        DataStream<String> source;
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
                       //WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)),
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source");

                if (query == 1) {
                	sink = KafkaSink.<AverageValue>builder()
                            .setBootstrapServers(bootstrapServer)
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic(output)
                                    .setValueSerializationSchema((SerializationSchema<AverageValue>) element -> element.toString().getBytes())
                                    .build()
                            )
                            .build();
                }
                else if (query==2) {
                	sink = KafkaSink.<AverageValue2>builder()
                            .setBootstrapServers(bootstrapServer)
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic(output)
                                    .setValueSerializationSchema((SerializationSchema<AverageValue2>) element -> element.toString().getBytes())
                                    .build()
                            )
                            .build();
                }
                else {
                	throw new IllegalArgumentException("The only supported queries are 1 and 2");
                }
                
            } else {
                throw new IllegalArgumentException("The only supported modes are \"file\" and \"kafka\".");
            }
        }

        System.out.println("[main] Source and Sink created.");

        DataStream<GridEvent> parser =
                source.flatMap(new ValueParser()).setParallelism(parallelism)
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<GridEvent>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkLateness)))  
                         //      .withTimestampAssigner((event, timestamp) -> event.getTimestamp()))
                        .name("parser");
        
        DataStream<GridEvent> parserNoWindows =
                source.flatMap(new ValueParser()).setParallelism(parallelism)
                        .name("parser");

        System.out.println("[main] Valueparser created.");

        DataStream<AverageValue> averageCalculator = null;
        DataStream<AverageValue2> averageCalculator2 = null;
        if (query==1) {
        	averageCalculator =
            		parser.keyBy(value -> value.getHouse())//.countWindowAll(slidingWindowSize, slidingWindowSlide)
                          // .window(SlidingEventTimeWindows.of(Time.seconds(slidingWindowSize), Time.seconds(slidingWindowSlide))) //.countWindow(3,1)
            				.window(SlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.seconds(slidingWindowSlide)))
                            .process(new AverageValueCalculator())
                            .setParallelism(parallelism)
                            .name("average-calculator");
        }
        else {
        	averageCalculator2 =
        			
            		parser.keyBy(value ->  ("" + value.getPlug() + value.getHousehold() + value.getHouse()) ) 
              
            				.window(SlidingProcessingTimeWindows.of(Time.seconds(slidingWindowSize), Time.seconds(slidingWindowSlide)))
                            .process(new AverageValueCalculator2())
                            .setParallelism(parallelism)
                            .name("average-calculator2");
        }
        

        System.out.println("[main] AverageValueCalculator created.");
        System.out.println("[main] SmartGrid created.");
        
        if (query==1) {
        	 if (mode.equals("file")) {
             	averageCalculator.sinkTo(sink).name("file-sink");
             }
             if (mode.equals("kafka")) {
            	 averageCalculator.sinkTo(sink).name("kafka-sink");
             }
        }
        else {
        	if (mode.equals("file")) {
            	averageCalculator2.sinkTo(sink).name("file-sink");
            }
            if (mode.equals("kafka")) {
            	averageCalculator2.sinkTo(sink).name("kafka-sink");
            }
        }
        

        System.out.println("[main] AverageValue sinks.");
        env.disableOperatorChaining();
        env.execute("Smart Grid");
    }

}
