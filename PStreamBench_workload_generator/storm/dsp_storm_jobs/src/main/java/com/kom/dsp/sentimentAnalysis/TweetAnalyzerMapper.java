package com.kom.dsp.sentimentAnalysis;

import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.tuple.Tuple;

import com.kom.dsp.sentimentAnalysis.SentimentResult.Sentiment;

public class TweetAnalyzerMapper implements TupleToKafkaMapper<Object, Object> {

    @Override
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    @Override
    public Object getMessageFromTuple(Tuple tuple) {
        String id = tuple.getStringByField("id");
        String timestamp = tuple.getStringByField("timestamp");
        String text = tuple.getStringByField("text");
        Sentiment sentiment = (Sentiment) tuple.getValueByField("sentiment");
        Double sentimentScore = tuple.getDoubleByField("score");
        return "TweetScored{" +
                "id='" + id + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", text='" + text + '\'' +
                ", sentiment='" + sentiment.toString() + '\'' +
                ", sentimentScore=" + sentimentScore +
                '}';
    }
}
