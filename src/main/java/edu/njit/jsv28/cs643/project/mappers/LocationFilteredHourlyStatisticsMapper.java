package edu.njit.jsv28.cs643.project.mappers;

import edu.njit.jsv28.cs643.project.utils.Constants;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class LocationFilteredHourlyStatisticsMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    public String pickupLocationId = "";
    public String dropoffLocationId = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pickupLocationId = context.getConfiguration().get("pickupLocationId");
        dropoffLocationId = context.getConfiguration().get("dropoffLocationId");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");

        if (parts[7].equals(pickupLocationId) && parts[8].equals(dropoffLocationId)) {
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
}
