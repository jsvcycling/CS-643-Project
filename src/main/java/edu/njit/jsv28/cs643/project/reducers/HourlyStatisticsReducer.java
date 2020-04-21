package edu.njit.jsv28.cs643.project.reducers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HourlyStatisticsReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        int totalPassengers = 0;
        float totalTripDistance = 0.f;
        float totalFareAmount = 0.f;
        float totalTipAmount = 0.f;
        float totalTotalAmount = 0.f;

        Map<Integer, Integer> pickupLocationIds = new HashMap<>();
        Map<Integer, Integer> dropoffLocationIds = new HashMap<>();

        for (MapWritable it : values) {
            count += 1;

            totalPassengers += ((IntWritable)it.get(new Text("passenger_count"))).get();
            totalTripDistance += ((FloatWritable)it.get(new Text("trip_distance"))).get();
            totalFareAmount += ((FloatWritable)it.get(new Text("fare_amount"))).get();
            totalTipAmount += ((FloatWritable)it.get(new Text("tip_amount"))).get();
            totalTotalAmount += ((FloatWritable)it.get(new Text("total_amount"))).get();

            int pickupLocationId = ((IntWritable)it.get(new Text("PULocationID"))).get();
            int dropoffLocationId = ((IntWritable)it.get(new Text("DOLocationID"))).get();

            pickupLocationIds.put(pickupLocationId, pickupLocationIds.getOrDefault(pickupLocationId, 0) + 1);
            dropoffLocationIds.put(dropoffLocationId, dropoffLocationIds.getOrDefault(dropoffLocationId, 0) + 1);
        }

        MapWritable result = new MapWritable();

        result.put(new Text("total_trips"), new IntWritable(count));

        //
        // Passenger Count Statistics
        //
        result.put(new Text("total_passengers"), new IntWritable(totalPassengers));
        result.put(new Text("mean_passenger_count"), new FloatWritable(totalPassengers / (float)count));

        //
        // Trip Distance Statistics
        //
        result.put(new Text("total_trip_length"), new FloatWritable(totalTripDistance));
        result.put(new Text("mean_trip_length"), new FloatWritable(totalTripDistance / (float)count));

        //
        // Fare Amount Statistics
        //
        result.put(new Text("total_fare_amount"), new FloatWritable(totalFareAmount));
        result.put(new Text("mean_fare_amount"), new FloatWritable(totalFareAmount / (float)count));

        //
        // Tip Amount Statistics
        //
        result.put(new Text("total_tip_amount"), new FloatWritable(totalTipAmount));
        result.put(new Text("mean_tip_amount"), new FloatWritable(totalTipAmount / (float)count));

        //
        // Total Amount Statistics
        //
        result.put(new Text("total_total_amount"), new FloatWritable(totalTotalAmount));
        result.put(new Text("mean_total_amount"), new FloatWritable(totalTotalAmount / (float)count));

        //
        // Pickup Location Statistics
        //
        // TODO

        //
        // Drop-off Location Statistics
        //
        // TODO

        context.write(key, result);
    }
}
