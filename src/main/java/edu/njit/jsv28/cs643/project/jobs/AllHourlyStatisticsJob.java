package edu.njit.jsv28.cs643.project.jobs;

import edu.njit.jsv28.cs643.project.mappers.AllHourlyStatisticsMapper;
import edu.njit.jsv28.cs643.project.reducers.HourlyStatisticsReducer;
import edu.njit.jsv28.cs643.project.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AllHourlyStatisticsJob {
    public static void main(String[] args) throws Exception {
        //
        // Initialize the job.
        //
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "All Hourly Statistics");
        job.setJarByClass(AllHourlyStatisticsJob.class);

        //
        // Configure the mapper.
        //
        job.setMapperClass(AllHourlyStatisticsMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        //
        // Configure the reducer.
        //
        job.setReducerClass(HourlyStatisticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        //
        // Finalize and run the job.
        //
        String outputPath = Constants.PATH_OUTPUT + "/all_hourly_statistics";

        FileInputFormat.setInputPaths(job, new Path(Constants.PATH_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
