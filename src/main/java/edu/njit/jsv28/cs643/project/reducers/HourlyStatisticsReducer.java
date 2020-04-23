package edu.njit.jsv28.cs643.project.reducers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HourlyStatisticsReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;

        int totalPassengers = 0;
        int minPassengers = Integer.MAX_VALUE;
        int maxPassengers = Integer.MIN_VALUE;

        float totalTripDistance = 0.f;
        float minTripDistance = Float.MAX_VALUE;
        float maxTripDistance = Float.MIN_VALUE;

        float totalFareAmount = 0.f;
        float minFareAmount = Float.MAX_VALUE;
        float maxFareAmount = Float.MIN_VALUE;

        float totalTipAmount = 0.f;
        float minTipAmount = Float.MAX_VALUE;
        float maxTipAmount = Float.MIN_VALUE;

        float totalTotalAmount = 0.f;
        float minTotalAmount = Float.MAX_VALUE;
        float maxTotalAmount = Float.MIN_VALUE;

        for (MapWritable it : values) {
            count += 1;

            int passengers = ((IntWritable)it.get(new Text("passenger_count"))).get();
            totalPassengers += passengers;
            minPassengers = Integer.min(minPassengers, passengers);
            maxPassengers = Integer.max(maxPassengers, passengers);

            float tripDistance = ((FloatWritable)it.get(new Text("trip_distance"))).get();
            totalTripDistance += tripDistance;
            minTripDistance = Float.min(minTripDistance, tripDistance);
            maxTripDistance = Float.max(maxTripDistance, tripDistance);

            float fareAmount = ((FloatWritable)it.get(new Text("fare_amount"))).get();
            totalFareAmount += fareAmount;
            minFareAmount = Float.min(minFareAmount, fareAmount);
            maxFareAmount = Float.max(maxFareAmount, fareAmount);

            float tipAmount = ((FloatWritable)it.get(new Text("tip_amount"))).get();
            totalTipAmount += tipAmount;
            minTipAmount = Float.min(minTipAmount, tipAmount);
            maxTipAmount = Float.max(maxTipAmount, tipAmount);

            float totalAmount = ((FloatWritable)it.get(new Text("total_amount"))).get();
            totalTotalAmount += totalAmount;
            minTotalAmount = Float.min(minTotalAmount, totalAmount);
            maxTotalAmount = Float.max(maxTotalAmount, totalAmount);
        }

        MapWritable result = new MapWritable();

        result.put(new Text("total_trips"), new IntWritable(count));

        //
        // Passenger Count Statistics
        //
        result.put(new Text("total_passengers"), new IntWritable(totalPassengers));
        result.put(new Text("mean_passengers"), new FloatWritable(totalPassengers / (float)count));
        result.put(new Text("min_passengers"), new IntWritable(minPassengers));
        result.put(new Text("max_passengers"), new IntWritable(maxPassengers));
        result.put(new Text("range_passengers"), new IntWritable(maxPassengers - minPassengers));

        //
        // Trip Distance Statistics
        //
        result.put(new Text("total_trip_length"), new FloatWritable(totalTripDistance));
        result.put(new Text("mean_trip_length"), new FloatWritable(totalTripDistance / (float)count));
        result.put(new Text("min_trip_length"), new FloatWritable(minTripDistance));
        result.put(new Text("max_trip_length"), new FloatWritable(maxTripDistance));
        result.put(new Text("range_trip_length"), new FloatWritable(maxTripDistance - minTripDistance));

        //
        // Fare Amount Statistics
        //
        result.put(new Text("total_fare_amount"), new FloatWritable(totalFareAmount));
        result.put(new Text("mean_fare_amount"), new FloatWritable(totalFareAmount / (float)count));
        result.put(new Text("min_fare_amount"), new FloatWritable(minFareAmount));
        result.put(new Text("max_fare_amount"), new FloatWritable(maxFareAmount));
        result.put(new Text("range_fare_amount"), new FloatWritable(maxFareAmount - minFareAmount));

        //
        // Tip Amount Statistics
        //
        result.put(new Text("total_tip_amount"), new FloatWritable(totalTipAmount));
        result.put(new Text("mean_tip_amount"), new FloatWritable(totalTipAmount / (float)count));
        result.put(new Text("min_tip_amount"), new FloatWritable(minTipAmount));
        result.put(new Text("max_tip_amount"), new FloatWritable(maxTipAmount));
        result.put(new Text("range_tip_amount"), new FloatWritable(maxTipAmount - minTipAmount));

        //
        // Total Amount Statistics
        //
        result.put(new Text("total_total_amount"), new FloatWritable(totalTotalAmount));
        result.put(new Text("mean_total_amount"), new FloatWritable(totalTotalAmount / (float)count));
        result.put(new Text("min_total_amount"), new FloatWritable(minTotalAmount));
        result.put(new Text("max_total_amount"), new FloatWritable(maxTotalAmount));
        result.put(new Text("range_total_amount"), new FloatWritable(maxTotalAmount - minTotalAmount));

        context.write(key, result);
    }
}
