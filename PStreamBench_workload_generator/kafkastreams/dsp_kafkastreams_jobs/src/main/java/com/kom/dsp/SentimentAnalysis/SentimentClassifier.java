package com.kom.dsp.SentimentAnalysis;

/**
 * Copied verbatim from spark/dsp_spark_jobs/.../SentimentAnalysis/SentimentClassifier.java.
 *
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize();
    public SentimentResult classify(String str);
}
