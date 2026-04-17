package com.kom.dsp.SpikeDetection;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SensorParser implements FlatMapFunction<String, SensorMeasurement> {

    private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS")
        .toFormatter();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void flatMap(String value, Collector<SensorMeasurement> out) throws Exception {
        String[] fields = value.split("\\s+");

        String dateStr = fields[0] + " " +fields[1];
        LocalDateTime date = LocalDateTime.parse(dateStr, formatterMillis);

        SensorMeasurement measurement = new SensorMeasurement(
                date.toEpochSecond(java.time.ZoneOffset.UTC), // timestamp
                Integer.parseInt(fields[2]), // sensorId
                Float.parseFloat(fields[3]), // temperature
                Float.parseFloat(fields[4]), // humidity
                Float.parseFloat(fields[5]), // light
                Float.parseFloat(fields[6])  // voltage
                );

        out.collect(measurement);
    }
}
