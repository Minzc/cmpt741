package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.AdvCli;
import ca.sfu.dataming.util.CliRunner;
import ca.sfu.dataming.util.DMConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author congzicun
 * @since 2014-11-10 10:46 AM
 */
public class SON implements CliRunner {
    public static final String MR_JOB_NAME_ONE = "SampleMR1";
    public static final String MR_JOB_NAME_TWO = "SampleMR2";
    public static final String JOB_NAME = "Son";
    private static final String TMP_NAME = "tmp";

    public static void main(String[] args) {
        AdvCli.initRunner(args, JOB_NAME, new SON());
    }

    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption(AdvCli.CLI_PARAM_I, true, "input path");
        options.addOption(AdvCli.CLI_PARAM_O, true, "output path");
        return options;
    }

    @Override
    public boolean validateOptions(CommandLine cmdLine) {
        return cmdLine.hasOption(AdvCli.CLI_PARAM_I) && cmdLine.hasOption(AdvCli.CLI_PARAM_O);
    }

    @Override
    public void start(CommandLine cmdLine) {
        try {
            String tmpPath = TMP_NAME + System.currentTimeMillis() / 1000;
            Job jobOne = new Job(DMConfiguration.getInstance());
            jobOne.setJobName(MR_JOB_NAME_ONE);
            jobOne.setJarByClass(SON.class);
            // set input & output
            FileInputFormat.setInputPaths(jobOne, cmdLine.getOptionValue(AdvCli.CLI_PARAM_I));
            FileOutputFormat.setOutputPath(jobOne, new Path(tmpPath));
            jobOne.setInputFormatClass(TextInputFormat.class);
            jobOne.setOutputFormatClass(TextOutputFormat.class);
            // set mappper
            jobOne.setMapperClass(SONMapperOne.class);
            jobOne.setMapOutputKeyClass(Text.class);
            jobOne.setMapOutputValueClass(IntWritable.class);
            // set Reducer
            jobOne.setNumReduceTasks(1);
            jobOne.setReducerClass(SONReducerOne.class);
            jobOne.setOutputKeyClass(Text.class);
            jobOne.setOutputValueClass(IntWritable.class);
            jobOne.waitForCompletion(true);


            Job jobTwo = new Job(DMConfiguration.getInstance());
            jobTwo.setJobName(MR_JOB_NAME_TWO);
            jobTwo.setJarByClass(SON.class);
            // set input & output
            FileInputFormat.setInputPaths(jobTwo, tmpPath);
            FileOutputFormat.setOutputPath(jobTwo, new Path(cmdLine.getOptionValue(AdvCli.CLI_PARAM_O)));
            jobTwo.setInputFormatClass(TextInputFormat.class);
            jobTwo.setOutputFormatClass(TextOutputFormat.class);
            // set mappper
            jobTwo.setMapperClass(SONMapperTwo.class);
            jobTwo.setMapOutputKeyClass(Text.class);
            jobTwo.setMapOutputValueClass(IntWritable.class);
            // set Reducer
            jobTwo.setNumReduceTasks(1);
            jobTwo.setReducerClass(SONReducerTwo.class);
            jobTwo.setOutputKeyClass(Text.class);
            jobTwo.setOutputValueClass(IntWritable.class);
            jobTwo.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
