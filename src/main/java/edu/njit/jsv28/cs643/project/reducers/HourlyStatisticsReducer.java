package edu.njit.jsv28.cs643.project.reducers;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HourlyStatisticsReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable result = new MapWritable();

        // TODO: Reduce data into statistics.

        context.write(key, result);
    }
}
