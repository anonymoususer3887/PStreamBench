package com.kom.dsp.AdsAnalytics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Q2AdEventParser implements FlatMapFunction<String, AdEvent> {
    @Override
    public void flatMap(String value, Collector<AdEvent> out) throws Exception {
        String[] record = value.split("\\s+");;
        int clicks = Integer.parseInt(record[0]);
        int views = Integer.parseInt(record[1]);
        String displayUrl = record[2];
        Long adId = Long.parseLong(record[3]);
        Long queryId = Long.parseLong(record[4]);
        int depth = Integer.parseInt(record[5]);
        int position = Integer.parseInt(record[6]);
        Long advertiserId = Long.parseLong(record[7]);
        Long keywordId = Long.parseLong(record[8]);
        Long titleId = Long.parseLong(record[9]);
        Long descriptionId = Long.parseLong(record[10]);
        Long userId = Long.parseLong(record[11]);

        AdEvent event = new AdEvent("", clicks, views, displayUrl, adId, advertiserId, depth, position, queryId, keywordId, titleId, descriptionId, userId, 1);
        out.collect(event);
    }
}
