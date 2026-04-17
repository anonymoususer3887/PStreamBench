package com.kom.dsp.smartgrid;

import com.kom.dsp.MachineOutlier.MachineUsage;
import com.kom.dsp.MachineOutlier.MachineUsageParser;
import com.kom.dsp.utils.Constants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SmartGridJob {

    public static void main(String[] args) throws Exception{
        // Creating Stream execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrapServer ;

        // Command line parameters

        String parallelism = params.get("parallelism").replace(" ","").replace("[","").replace("]","").replace("'","");
        String[] parallelism_degree = parallelism.split(",");

        //int parallelism = Integer.parseInt(params.get("parallelism"));
        int query = Integer.parseInt(params.get("query"));
        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        int watermarkLateness = Integer.parseInt(params.get("lateness"));
        int topicPopularityThreshold = Integer.parseInt(params.get("popularityThreshold"));
        env.setParallelism(Integer.parseInt(parallelism_degree[0]));
        //{'job_run_time': 120, 'job_program': 'com.kom.dsp.smartgrid.SmartGridJob',
        // 'job_parallelization': '2', 'google_lateness': 0, 'job_query_number': 2,
        // 'job_mode': 'Kafka', 'job_window_size': 100, 'job_window_slide_size': 10,
        // 'producer_program': 'Smart_Grid', 'producer_event_per_second': '1000',
        // 'producer_bootstrap_server': 'node0.test23.maki-test.emulab.net:9092',
        // 'job_input': 'SmartGridIn', 'job_output': 'SmartGridOut'}


        DataStream<String> source = null;
        Sink sink = null;
        if (mode.equalsIgnoreCase(Constants.FILE)) {
            System.out.println("[main] Arguments parsed.");

        } else if (mode.equalsIgnoreCase(Constants.KAFKA)) {
                bootstrapServer = params.get("kafka-server");
                System.out.println("[main] Arguments parsed.");
                KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setTopics(input)
                        .setGroupId("my-group")
                        .setProperty("fetch.min.bytes", "100000")
                        .setProperty("fetch.max.wait.ms", "50")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .build();
                source = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source")
                        .setParallelism(Integer.parseInt(parallelism_degree[0]));

                sink = KafkaSink.<ProcessOutputQ1>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                //.setValueSerializationSchema(new SimpleStringSchema())
                                .setValueSerializationSchema(new ProcessOutputQ1SerializationSchema())
                                .build()
                        )
                        .setProperty("batch.size", "200000")
                        .setProperty("batch.num.messages", "200000")
                        .setProperty("linger.ms", "10")
                        .setProperty("compression.type", "lz4")
                        .setProperty("acks", "1")
                        .build();

        }

        // Running query
        DataStream<HouseEvent> houseEventSource = source
            .map(new HouseEventParser())
            .name("house-event-parser")
            .setParallelism(Integer.parseInt(parallelism_degree[1]));

        if(query == Constants.QUERY_ONE){

            DataStream<ProcessOutputQ1> processedOutputQ1 = houseEventSource
                .keyBy(HouseEvent::getHouse)
                .window(SlidingProcessingTimeWindows.of(
                //.windowAll(SlidingProcessingTimeWindows.of(
                            Time.milliseconds(slidingWindowSize),
                            Time.milliseconds(slidingWindowSlide)))
                .process(new ProcessWindowQ1())
                //.aggregate(new ProcessWindowAllQ1())
                .name("global average load")
                .setParallelism(Integer.parseInt(parallelism_degree[2]));
                //.setParallelism(1);

            //DataStream<String> output2Sink = processedOutputQ1
            //    .process(new Q1Parser())
            //    .name("output")
            //    .setParallelism(Integer.parseInt(parallelism_degree[3]));

            processedOutputQ1
                .sinkTo(sink)
                .name("kafka-sink")
                .setParallelism(Integer.parseInt(parallelism_degree[3]));

        } else if(query == Constants.QUERY_TWO) {

            DataStream<ProcessOutputQ2> processedOutputQ2 = houseEventSource
                    .keyBy(new KeySelector<HouseEvent, Tuple3<Long, Long, Long>>() {
                        @Override
                        public Tuple3<Long, Long, Long> getKey(HouseEvent value) throws Exception {
                            return Tuple3.of(
                                    value.getHouse(),
                                    value.getHouseholds(),
                                    value.getPlugs());
                        }
                    })
                    .window(SlidingProcessingTimeWindows.of(
                                Time.seconds(slidingWindowSize),
                                Time.seconds(slidingWindowSlide)))
                    .process(new ProcessWindowQ2())
                    .name("local average load")
                    .setParallelism(Integer.parseInt(parallelism_degree[2]));

            //DataStream<String> output2Sink = processedOutputQ2
            //    .process(new Q2Parser())
            //    .name("output")
            //    .setParallelism(Integer.parseInt(parallelism_degree[3]));

            processedOutputQ2
                .sinkTo(sink)
                .name("kafka-sink")
                .setParallelism(Integer.parseInt(parallelism_degree[4]));
        }

        env.disableOperatorChaining();
        //env.execute("Flink smart grid job");

        JobClient client = env.executeAsync("Flink smart grid job");
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
