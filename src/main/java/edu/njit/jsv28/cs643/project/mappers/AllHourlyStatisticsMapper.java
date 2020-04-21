package edu.njit.jsv28.cs643.project.mappers;

import edu.njit.jsv28.cs643.project.utils.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class AllHourlyStatisticsMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");

        try {
            Integer.parseInt(parts[7]);
            Integer.parseInt(parts[8]);
        } catch (NumberFormatException e) {
            return;
        }

        //
        // Generate the hour-of-day string.
        //
        int pickupHour = LocalDateTime.parse(parts[1], Constants.TIMESTAMP_FORMAT).getHour();
        String pickupTime = LocalTime.of(pickupHour, 0).format(Constants.TIME_FORMAT);

        MapWritable result = new MapWritable();

        // TODO: Select fields.

        context.write(new Text(pickupTime), result);
    }
}
