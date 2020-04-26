package edu.njit.cs643.group3.mappers;

import edu.njit.cs643.group3.utils.Constants;
import org.apache.hadoop.io.*;
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

        try {
            int passengerCount = Integer.parseInt(parts[3]);
            float tripDistance = Float.parseFloat(parts[4]);
            float fareAmount = Float.parseFloat(parts[10]);
            float tipAmount = Float.parseFloat(parts[13]);
            float totalAmount = Float.parseFloat(parts[16]);

            result.put(new Text("passenger_count"), new IntWritable(passengerCount));
            result.put(new Text("trip_distance"), new FloatWritable(tripDistance));
            result.put(new Text("fare_amount"), new FloatWritable(fareAmount));
            result.put(new Text("tip_amount"), new FloatWritable(tipAmount));
            result.put(new Text("total_amount"), new FloatWritable(totalAmount));

            context.write(new Text(pickupTime), result);
        } catch (Exception e) {
            // Do nothing.
        }
    }
}
