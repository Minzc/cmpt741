package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.DMConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author congzicun
 * @since 2014-11-02 3:12 PM
 */
public class SampleMR {
    public static final String MR_JOB_NAME = "SampleMR";
    private static final String TMP_NAME = "tmp";

    public static void main(String[] args) throws IOException {
        Job job = new Job(DMConfiguration.getInstance());
        job.setJobName(MR_JOB_NAME);
        job.setJarByClass(SampleMR.class);
        // set input & output
        FileInputFormat.setInputPaths(job, TMP_NAME);
        FileOutputFormat.setOutputPath(job, new Path(TMP_NAME));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // set mappper
        job.setMapperClass(SampleMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // set Reducer
        job.setNumReduceTasks(1);
        job.setReducerClass(SampleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
    }

    private class SampleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("Hello World"), new LongWritable(1));
            context.write(new Text("Hello World"), new LongWritable(1));
            context.write(new Text("Hello World"), new LongWritable(1));
            context.write(new Text("Hello World"), new LongWritable(1));
            context.write(new Text("Hello World"), new LongWritable(1));
            context.write(new Text("Hello World"), new LongWritable(1));
        }
    }

    private class SampleReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable value : values) {
                counter += value.get();
            }
            context.write(key, new LongWritable(counter));
        }
    }
}
