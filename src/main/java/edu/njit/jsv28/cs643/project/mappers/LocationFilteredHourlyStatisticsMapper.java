package edu.njit.jsv28.cs643.project.mappers;

import edu.njit.jsv28.cs643.project.utils.Constants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
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

            // TODO: Select fields.

            context.write(new Text(pickupTime), result);
        }
    }
}
