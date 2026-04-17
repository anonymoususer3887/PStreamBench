package SpikeDetection;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

class SensorParser implements FlatMapFunction<String, SensorMeasurement> {
    private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
            .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
            .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
            .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
            .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();

    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void flatMap(String value, Collector<SensorMeasurement> out) throws Exception {
    	//System.out.println(value);
        String[] fields = value.split("\\s+");

        if (fields.length == 8) {
            //String dateStr = String.format("%s %s", 0, 1);
        	String dateStr = fields[0] + " " +fields[1];
            DateTime date = formatterMillis.parseDateTime("2004-02-28 01:20:16.567523");
            //System.out.println(dateStr);

            try {
                date = formatterMillis.parseDateTime(dateStr);
            } catch (IllegalArgumentException ex) {
                try {
                    date = formatter.parseDateTime(dateStr);
                } catch (IllegalArgumentException ex2) {
                    System.out.println("Error parsing record date/time field, input record: " + value);
                }
            }
            try {
                out.collect(new SensorMeasurement(date.getMillis(), Integer.parseInt(fields[2]),
                        Integer.parseInt(fields[3]),
                        Float.parseFloat(fields[4]),
                        Float.parseFloat(fields[5]),
                        Float.parseFloat(fields[6]),
                        Float.parseFloat(fields[7])));
            } catch (NumberFormatException ex) {
                System.out.println("Error parsing record numeric field, input record: " + value);
            }
        }
    }
}
