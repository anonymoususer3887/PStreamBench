package com.kom.dsp.AdsAnalytics;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Q2AdEventCounter implements FlatMapFunction<AdEvent, RollingCTR> {
    private Map<String, Integer> clickCounts;
    private Map<String, Integer> impressionCounts;

    public Q2AdEventCounter() {
        this.clickCounts = new HashMap<>();
        this.impressionCounts = new HashMap<>();
    }

    @Override
    public void flatMap(AdEvent value, Collector<RollingCTR> out) throws Exception {
        Long queryId = value.getQueryId();
        Long adId = value.getAdId();
        Integer clicks = value.getClicks();
        Integer views = value.getViews();

        // Build a key to identify the ad event
        String key = queryId + "-" + adId;

        // Update click or impression counts based on the type of event
        clickCounts.put(key, clickCounts.getOrDefault(key, 0) + clicks);
        impressionCounts.put(key, clickCounts.getOrDefault(key, 0) + views);

        // Calculate CTR for the ad event
        Integer currentClicks = clickCounts.getOrDefault(key, 0);
        Integer currentImpressions = impressionCounts.getOrDefault(key, 0);
        float ctrValue = calculateCTR(currentClicks, currentImpressions);

        RollingCTR rollingCTR = new RollingCTR(queryId, adId, currentClicks, currentImpressions, ctrValue);
        out.collect(rollingCTR);
    }

    private float calculateCTR(int clicks, int impressions) {
        if (impressions == 0) {
            return 0.0f; // Avoid division by zero
        }
        return (float) clicks / impressions;
    }
}
