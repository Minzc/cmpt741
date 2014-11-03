package ca.sfu.dataming.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @author congzicun
 * @since 2014-11-02 3:28 PM
 */
public class AbstractJob extends Configured implements Tool, JobHelper.MRJob {
    // ============== Local Functions ===================

    protected Job createJob(String commaSeparatedPaths, String outputPath)
            throws IOException {
        Job job = new Job(DMConfiguration.getInstance());
        job.setJobName("Abstract Job: this should not be run in release mode.");
        job.setJarByClass(AbstractJob.class);
        // set input & output
        if (commaSeparatedPaths != null)
            FileInputFormat.setInputPaths(job, commaSeparatedPaths);
        if (outputPath != null)
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // set mappper
//        job.setMapperClass(CommonMR.CommonTextMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
        // set Reducer
//        job.setReducerClass(CommonMR.DuplicateRmoverReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = createJob(args[0], args[1]);
        job.waitForCompletion(true);
        return 0;
    }

    @Override
    public Job createJob(String[] args) throws IOException {
        return createJob(args[0], args[1]);
    }
}

