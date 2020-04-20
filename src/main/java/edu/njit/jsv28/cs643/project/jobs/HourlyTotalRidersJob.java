package edu.njit.jsv28.cs643.project.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import edu.njit.jsv28.cs643.project.mappers.YellowCabDataToTimeOfDayMapper;
import edu.njit.jsv28.cs643.project.utils.Constants;

public class HourlyTotalRidersJob {
    public static void main(String[] args) throws Exception {
        //
        // Initialize the job.
        //
        Configuration conf = new Configuration();

        conf.set("pickupLocationId", args[0]);
        conf.set("dropoffLocationId", args[1]);

        Job job = Job.getInstance(conf, "HourlyTotalRidersJob");
        job.setJarByClass(HourlyTotalRidersJob.class);

        //
        // Configure the mapper.
        //
        job.setMapperClass(YellowCabDataToTimeOfDayMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //
        // Configure the reducer.
        //
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //
        // Finalize and run the job.
        //
        FileInputFormat.setInputPaths(job, new Path(Constants.PATH_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Constants.PATH_OUTPUT + "/hourly_total_riders"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
