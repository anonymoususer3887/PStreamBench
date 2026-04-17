package com.kom.dsp.logsAnalytics;

public class LogEvent {
    private String logTime;
    private String statusCode;

    public LogEvent(String logTime, String statusCode) {
        this.logTime = logTime;
        this.statusCode = statusCode;
    }

    public String getLogTime() {
        return logTime;
    }

    public void setLogTime(String logTime) {
        this.logTime = logTime;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }
}
