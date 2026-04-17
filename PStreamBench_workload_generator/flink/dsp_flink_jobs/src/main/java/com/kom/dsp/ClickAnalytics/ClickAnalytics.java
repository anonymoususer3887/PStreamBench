package com.kom.dsp.ClickAnalytics;

import com.kom.dsp.SentimentAnalysis.Tweet;
import com.kom.dsp.SentimentAnalysis.TweetScored;

import com.kom.dsp.smartgrid.ProcessOutputQ1;
import com.kom.dsp.smartgrid.ProcessOutputQ2;
import com.kom.dsp.utils.Constants;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.naming.OperationNotSupportedException;

public class ClickAnalytics {



    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(10);
        System.out.println("[main] Execution environment created.");

        ParameterTool params = ParameterTool.fromArgs(args);

        //int parallelism = Integer.parseInt(params.get("parallelism"));
        int query = Integer.parseInt(params.get("query"));
        //int parallelism = Integer.parseInt(params.get("parallelism"));
        String parallelism = params.get("parallelism").replace(" ","").replace("[","").replace("]","").replace("'","");
        String[] parallelism_degree = parallelism.split(",");
        String mode = params.get("mode");
        String input = params.get("input");
        String output = params.get("output");
        String bootstrapServer;
        long secondsToWait = Long.parseLong(params.get("waitTimeToCancel"));
        int slidingWindowSize = Integer.parseInt(params.get("size")); // important to implement
        int slidingWindowSlide = Integer.parseInt(params.get("slide"));
        int watermarkLateness = Integer.parseInt(params.get("lateness"));
        int topicPopularityThreshold = Integer.parseInt(params.get("popularityThreshold"));
        env.setParallelism(Integer.parseInt(parallelism_degree[0]));


        DataStream<String> source;
        Sink sink = null;
        if (mode.equalsIgnoreCase(Constants.FILE)) {
            System.out.println("[main] Arguments parsed.");
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(new TextLineFormat(), new Path(input))
                    .monitorContinuously(Duration.ofSeconds(10))
                    .build();
            source = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-input");

            sink = FileSink.<TweetScored>forRowFormat(
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



                if(query == Constants.QUERY_ONE || query == Constants.QUERY_THREE){

                    sink = KafkaSink.<Tuple3<String, Integer, Integer>>builder()
                            .setBootstrapServers(bootstrapServer)
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic(output)
                                    .setValueSerializationSchema((SerializationSchema<Tuple3<String, Integer,Integer>>) element -> element.toString().getBytes(StandardCharsets.UTF_8))
                                    .build()
                            )
                            .setProperty("batch.size", "200000")
                            .setProperty("batch.num.messages", "200000")
                            .setProperty("linger.ms", "10")
                            .setProperty("compression.type", "lz4")
                            .setProperty("acks", "1")
                            .build();

                }
                else if(query == Constants.QUERY_TWO){

                    sink = KafkaSink.<GeoStats>builder()
                            .setBootstrapServers(bootstrapServer)
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic(output)
                                    .setValueSerializationSchema((SerializationSchema<GeoStats>) element -> element.toString().getBytes(StandardCharsets.UTF_8))
                                    .build()
                            )
                            .setProperty("batch.size", "200000")
                            .setProperty("batch.num.messages", "200000")
                            .setProperty("linger.ms", "10")
                            .setProperty("compression.type", "lz4")
                            .setProperty("acks", "1")
                            .build();

                }
            } else {
                throw new IllegalArgumentException("The only supported modes are \"file\" and \"kafka\".");
            }
        }

        System.out.println("[main] Source and Sink created.");

        DataStream<ClickLog> parsedClickLogEvent =
                source.map(new ClickLogParser()).name("click-log-parser").setParallelism(Integer.parseInt(parallelism_degree[1]));

        System.out.println("[main] Parser created.");

        if(query == Constants.QUERY_ONE){
            // Process repeat visits
            DataStream<Tuple3<String,Integer,Integer>> repeatVisitStream = parsedClickLogEvent
                .keyBy(ClickLog::getClientKey)
                .window(SlidingProcessingTimeWindows.of(
                            Time.milliseconds(slidingWindowSize),
                            Time.milliseconds(slidingWindowSlide)))
                .process(new RepeatVisitOperator())
                .name("repeat-visit")
                .setParallelism(Integer.parseInt(parallelism_degree[2]));

            DataStream<Tuple3<String,Integer,Integer>> resultStream = repeatVisitStream
                .keyBy(value -> value.f0)
                .reduce(new SumReducer())
                .name("reduce-operation")
                .setParallelism(Integer.parseInt(parallelism_degree[3]));

            System.out.println("[main] repeatVisitStream created.");

            if (mode.equalsIgnoreCase(Constants.FILE)) {
                resultStream.sinkTo(sink).name("file-sink");
            }
            if (mode.equalsIgnoreCase(Constants.KAFKA)) {
                resultStream.sinkTo(sink).name("kafka-sink")
                    .setParallelism(Integer.parseInt(parallelism_degree[4]));
            }
        } else if (query == Constants.QUERY_TWO){
            // Process geography
            DataStream<GeoStats> geographyStream = parsedClickLogEvent
                    .process(new GeographyOperator())
                    .name("geography-visit")
                    .setParallelism(Integer.parseInt(parallelism_degree[2]));

            System.out.println("[main] geographyStream created.");

            if (mode.equalsIgnoreCase(Constants.FILE)) {
                geographyStream.sinkTo(sink).name("file-sink");
            }
            if (mode.equalsIgnoreCase(Constants.KAFKA)) {
                geographyStream.sinkTo(sink).name("kafka-sink")
                    .setParallelism(Integer.parseInt(parallelism_degree[3]));
            }
        } else if (query == Constants.QUERY_THREE){
            //throw new OperationNotSupportedException();

            // Process repeat visits
            DataStream<Tuple3<String,Integer,Integer>> repeatVisitStream = parsedClickLogEvent
                .keyBy(ClickLog::getClientKey)
                .countWindow(slidingWindowSize, slidingWindowSlide)
                .process(new RepeatVisitCountOperator())
                .name("repeat-visit")
                .setParallelism(Integer.parseInt(parallelism_degree[2]));

            DataStream<Tuple3<String,Integer,Integer>> resultStream = repeatVisitStream
                .keyBy(value -> value.f0)
                .reduce(new SumReducer())
                .name("reduce-operation")
                .setParallelism(Integer.parseInt(parallelism_degree[3]));

            System.out.println("[main] repeatVisitStream created.");

            if (mode.equalsIgnoreCase(Constants.FILE)) {
                resultStream.sinkTo(sink).name("file-sink");
            }
            if (mode.equalsIgnoreCase(Constants.KAFKA)) {
                resultStream.sinkTo(sink).name("kafka-sink")
                    .setParallelism(Integer.parseInt(parallelism_degree[4]));
            }
        }



        System.out.println("[main] Analyser sinks.");
        env.disableOperatorChaining();
        //env.execute("Sentiment Analysis");

        JobClient client = env.executeAsync("Click Analytics");
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
