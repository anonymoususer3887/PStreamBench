package SmartGrid;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import SmartGrid.GridEvent;

class ValueParser implements FlatMapFunction<String, GridEvent> {
    

    @Override
    public void flatMap(String value, Collector<GridEvent> out) throws Exception {
        String[] fields = value.split("\\s+");
        if (fields.length == 7) {
            
            try {
            	GridEvent g = new GridEvent( Integer.parseInt(fields[0]),
                        Long.parseLong(fields[1]),
                        Float.parseFloat(fields[2]),
                        Integer.parseInt(fields[3]),
                        Integer.parseInt(fields[4]),
                        Integer.parseInt(fields[5]),
                        Integer.parseInt(fields[6]));
                out.collect(new GridEvent( Integer.parseInt(fields[0]),
                        Long.parseLong(fields[1]),
                        Float.parseFloat(fields[2]),
                        Integer.parseInt(fields[3]),
                        Integer.parseInt(fields[4]),
                        Integer.parseInt(fields[5]),
                        Integer.parseInt(fields[6])));
               
            } catch (NumberFormatException ex) {
                System.out.println("Error parsing record numeric field, input record: " + value);
            }
            
        }
    }
}
