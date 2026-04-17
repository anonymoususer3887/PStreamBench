package WordCount;

import org.apache.commons.configuration2.Configuration;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.metrics.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.io.FileWriter;
import java.io.IOException;



public class WordCount {
	static ArrayList<String> listOutput = new ArrayList<>();
    public static void main(String[] args) throws Exception {
    	
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new HashMapStateBackend());
        env.getConfig().setAutoWatermarkInterval(1000);
        env.getConfig().setLatencyTrackingInterval(10);
        
        
        
        
        System.out.println("[main] Execution environment created.");
        ParameterTool params = ParameterTool.fromArgs(args);

        int parallelism = Integer.parseInt(params.get("parallelism"));
        String mode = params.get("mode"); System.out.println(mode);
        String input = params.get("input");
        String output = params.get("output");
        String bootstrapServer;
        int num_record = Integer.parseInt(params.get("num-record"));
        int throughput = Integer.parseInt(params.get("throughput"));
        String output2 = output+".txt";
        
        /*int parallelism = 1;
        String mode = "kafka";
        String input = "wordCount-input";
        String output = "wordCount-output";
        String bootstrapServer = "localhost:9092";*/
        
        /*int parallelism = 1;
        String mode = "file";
        String input = "/home/moayad/MMC_Project/t2_ws2022_23-main/Data/task2/wordCount.dat";
        String output = "/home/moayad/MMC_Project/t2_ws2022_23-main/Data/task2/wordCountOut";
        String output2 = "/home/moayad/MMC_Project/t2_ws2022_23-main/Data/task2/wordCountOut.txt";
        String bootstrapServer = "localhost:9092";
        int num_record = 20;
        int throughput = 1;*/
        

        DataStream<String> source;
        FileSink<Tuple2<String, Integer>> fileSink = null;
        KafkaSink<Tuple2<String, Integer>> kafkaSink = null;
        if (mode.equals("file")) {
            System.out.println("[main] Arguments parsed.");
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                WordCount.writeIntoFile(output2);
            }, 0, 1, TimeUnit.SECONDS);
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineFormat(), new Path(input))
                    .monitorContinuously(Duration.ofSeconds(10))
                    .build();
            source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input").flatMap(new FlatMapFunction<String, String>() {
           	   private int count = 0;

          	   @Override
          	   public void flatMap(String value, Collector<String> out) throws Exception {
          	      if (count < num_record) {
          	    	 Thread.sleep(1000/throughput);
          	         out.collect(value);
          	         count++;
          	      }
          	   }
          	});

            fileSink = FileSink.<Tuple2<String, Integer>>forRowFormat(
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
                //bootstrapServer = params.get("kafka-server");
                bootstrapServer = "localhost:9092";
                System.out.println("[main] Arguments parsed.");
                KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setTopics(input)
                        .setGroupId("my-group")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setProperty("fetch.min.bytes", "100000")
                        .setProperty("fetch.max.wait.ms", "50")
                        .build();
                source = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source");

                kafkaSink = KafkaSink.<Tuple2<String, Integer>>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                .setValueSerializationSchema((SerializationSchema<Tuple2<String, Integer>>) element -> element.toString().getBytes(StandardCharsets.UTF_8))
                                .build()
                        )
                        //.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("batch.size", "200000")
                        .setProperty("batch.num.messages", "200000")
                        .setProperty("linger.ms", "10")
                        .setProperty("compression.type", "lz4")
                        .setProperty("acks", "0")
                        .build();
            } else {
                throw new IllegalArgumentException("The only supported modes are \"file\" and \"kafka\".");
            }
        }

        System.out.println("[main] Source and Sink created.");

        DataStream<Tuple2<String, Integer>> tokens =
                source.flatMap(new Splitter()).setParallelism(parallelism).name("tokenizer");

        System.out.println("[main] Tokenizer created.");

        DataStream<Tuple2<String, Integer>> counter =
               tokens.keyBy(value -> value.f0). sum(1).setParallelism(parallelism).name("counter");

        System.out.println("[main] Counter created.");

        if (mode.equals("file")) {
            counter.sinkTo(fileSink).name("file-sink");
            counter.addSink(new SinkFunction<Tuple2<String, Integer>>() {
                @Override
                public void invoke(Tuple2<String, Integer> value, Context context) throws Exception{
                    System.out.println("(" + value.f0 + ", " + value.f1 + ")");
                    listOutput.add("(" + value.f0 + ", " + value.f1 + ")");
                }
            });
        }

        if (mode.equals("kafka")) {
            counter.sinkTo(kafkaSink).name("kafka-sink");
        }
        
        
        System.out.println("[main] Counter sinks.");
        
       
        env.disableOperatorChaining();
        JobExecutionResult result = env.execute("Word Count");
    }
    
    static void writeIntoFile(String output2) {
    	
    	try {
            FileWriter writer = new FileWriter(output2);
            for (String str : listOutput) {
                writer.write(str + System.lineSeparator());
                
            }
            writer.close();
            //System.out.println("Successfully wrote ArrayList to file.");
        } catch (IOException e) {
            //System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}


