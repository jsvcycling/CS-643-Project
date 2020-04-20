package edu.njit.jsv28.cs643.project.mappers;

import edu.njit.jsv28.cs643.project.utils.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class YellowCabDataToTimeOfDayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private String pickupLocationId = "";
    private String dropoffLocationId = "";

    @Override
    protected void setup(Context context) {
        pickupLocationId = context.getConfiguration().get("pickupLocationId");
        dropoffLocationId = context.getConfiguration().get("dropoffLocationId");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");

        // If the pickup and drop-off location match the input arguments, find
        // the hour of day that the pickup occurred and the number of
        // passengers.
        if (parts[7].equals(pickupLocationId) && parts[8].equals(dropoffLocationId)) {
            int pickupHour = LocalDateTime.parse(parts[1], Constants.TIMESTAMP_FORMAT).getHour();
            int passengerCount = Integer.parseInt(parts[3]);

            // Convert the hours to "HH:mm" format.
            String pickupTime = LocalTime.of(pickupHour, 0).format(Constants.TIME_FORMAT);

            context.write(new Text(pickupTime), new IntWritable(passengerCount));
        }
    }
}
