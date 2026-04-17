package com.kom.dsp.TPCH;

import com.kom.dsp.BargainIndex.*;
import com.kom.dsp.TPCH.FormatterOutput;
import com.kom.dsp.TrafficMonitoring.AverageSpeedCalculator;
import com.kom.dsp.TrafficMonitoring.TrafficEventWithRoad;
import com.kom.dsp.utils.Constants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.CompletableFuture;

import javax.naming.OperationNotSupportedException;

public class TPCH {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(10);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        System.out.println("[main] Execution environment created.");

        ParameterTool params = ParameterTool.fromArgs(args);

        String parallelism = params.get("parallelism").replace(" ","").replace("[","").replace("]","").replace("'","");
        String[] parallelism_degree = parallelism.split(",");
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String bootstrapServer = params.get("kafka-server");;
        int query = Integer.parseInt(params.get("query"));
        int slidingWindowSize = Integer.parseInt(params.get("size"));
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        int watermarkLateness = Integer.parseInt(params.get("lateness"));
        int topicPopularityThreshold = Integer.parseInt(params.get("popularityThreshold"));
        env.setParallelism(Integer.parseInt(parallelism_degree[0]));


        DataStream<String> source = null;
        Sink sink = null;
        if (mode.equalsIgnoreCase(Constants.FILE)) {
        } else {
            if (mode.equalsIgnoreCase(Constants.KAFKA)) {

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
                        "kafka-source").setParallelism(Integer.parseInt(parallelism_degree[0]));

                sink = KafkaSink.<Tuple2<Integer, Integer>>builder()
                        .setBootstrapServers(bootstrapServer)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(output)
                                .setValueSerializationSchema(new TPCHSerializationSchema())
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

        DataStream<TPCHEvent> parsedStream =
                source.map(new TPCHEventParser()).name("tpch-event-parser").setParallelism(Integer.parseInt(parallelism_degree[1]));

        DataStream<TPCHEvent> filteredData;

        if(query == Constants.QUERY_ONE) {
            filteredData = parsedStream
                    .filter(event -> event.getOrderPriority() > 2)
                    .name("tpch-data-filter")
                    .setParallelism(Integer.parseInt(parallelism_degree[2]));
        } else if(query == Constants.QUERY_TWO) {
            filteredData = parsedStream
                    //.filter(orderPriority -> orderPriority.compareTo("1-URGENT") >= 0 && orderPriority.compareTo("5-LOW") <= 0)
                    .keyBy(TPCHEvent::getDiscount) // Key the data stream by roadId
                    .window(SlidingProcessingTimeWindows.of(
                                Time.milliseconds(slidingWindowSize),
                                Time.milliseconds(slidingWindowSlide)))
                    .process(new FilterCalculator())
                    //.filter(orderPriority -> orderPriority.getOrderPriority() >= 1 && orderPriority.getOrderPriority() <= 5)
                    .name("tpch-data-filter")
                    .setParallelism(Integer.parseInt(parallelism_degree[2]));
        } else {
            throw new OperationNotSupportedException("selected query does not exist");
        }

        // Calculate the count for each order priority
        DataStream<Tuple2<Integer, Integer>> orderPriorityCount = filteredData
                .flatMap(new PriorityMapper())
                .name("tpch-flat-mapper")
                .setParallelism(Integer.parseInt(parallelism_degree[3])); // Calculate the count of orders for each priority

        DataStream<Tuple2<Integer, Integer>> priorityCounter = orderPriorityCount
                .keyBy(data -> data.f0)   // key by orderPriority
                .sum(1)                   // sum price
                .name("priority-counter")
                .setParallelism(Integer.parseInt(parallelism_degree[4]));

        if (mode.equalsIgnoreCase(Constants.FILE)) {
            priorityCounter.sinkTo(sink).name("file-sink");
        }
        if (mode.equalsIgnoreCase(Constants.KAFKA)) {
            priorityCounter.sinkTo(sink).name("kafka-sink")
                .setParallelism(Integer.parseInt(parallelism_degree[5]));
        }

        System.out.println("[main] sink created.");

        env.disableOperatorChaining();


        JobClient client = env.executeAsync("TPCH");
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
