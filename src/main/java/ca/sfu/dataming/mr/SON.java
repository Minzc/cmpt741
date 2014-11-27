package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;


/**
 * @author congzicun
 * @since 2014-11-10 10:46 AM
 */
public class SON implements CliRunner {
    public static final String MR_JOB_NAME_ONE = "SampleMR1";
    public static final String MR_JOB_NAME_TWO = "SampleMR2";
    public static final String JOB_NAME = "Son";
    private static final String TMP_NAME = "tmp";
    private static final String CLI_PARAM_L = "l";
    private static final String CLI_PARAM_R1 = "r1";
    private static final String CLI_PARAM_R2 = "r2";

    public static void main(String[] args) {
        AdvCli.initRunner(args, JOB_NAME, new SON());
    }

    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption(AdvCli.CLI_PARAM_I, true, "input path");
        options.addOption(AdvCli.CLI_PARAM_O, true, "output path");
        options.addOption(AdvCli.CLI_PARAM_S, true, "threshold");
        options.addOption(CLI_PARAM_R1, true, "number of lines per chunk");
        options.addOption(CLI_PARAM_R2, true, "number of lines per chunk");
        options.addOption(CLI_PARAM_L, true, "bytes of data processed by each map");
        return options;
    }

    @Override
    public boolean validateOptions(CommandLine cmdLine) {
        return cmdLine.hasOption(AdvCli.CLI_PARAM_I) && cmdLine.hasOption(AdvCli.CLI_PARAM_O) && cmdLine.hasOption(AdvCli.CLI_PARAM_S);
    }

    @Override
    public void start(CommandLine cmdLine) {
        try {

            long startTime = System.currentTimeMillis();
            // initialization and set up environment
            Configuration conf = DMConfiguration.getInstance();
            String tmpPath = TMP_NAME + System.currentTimeMillis() / 1000;
            conf.set(DMConsts.C_ITEMSETS_PATH, tmpPath);
            conf.set(DMConsts.SON_SUPPORT_THRSHLD, cmdLine.getOptionValue(AdvCli.CLI_PARAM_S));
            String inputPath = cmdLine.getOptionValue(AdvCli.CLI_PARAM_I);
            String outputPath = cmdLine.getOptionValue(AdvCli.CLI_PARAM_O);
            if (cmdLine.getOptionValue(CLI_PARAM_L) != null) {
                long splitSize = HDFSUtil.getFileSize(inputPath) / Integer.parseInt(cmdLine.getOptionValue(CLI_PARAM_L));
                conf.set(DMConsts.MAPRED_MAX_SPLIT_SIZE, Long.toString(splitSize));
            }

            // create and execute job one
            Job jobOne = getDefaultJob(conf, MR_JOB_NAME_ONE, inputPath, tmpPath);
            // set mappper
            jobOne.setMapperClass(AprioriMapperOne.class);
            jobOne.setMapOutputValueClass(NullWritable.class);
            // set Reducer
            jobOne.setReducerClass(SONReducerOne.class);
            if (cmdLine.getOptionValue(CLI_PARAM_R1) != null) {
                jobOne.setNumReduceTasks(Integer.parseInt(cmdLine.getOptionValue(CLI_PARAM_R1)));
            }
            // make sure the ouputpath is not occupied by other files
            HDFSUtil.delFile(tmpPath);
            jobOne.waitForCompletion(true);

            System.out.println("+--------------------+");
            System.out.println("Step one finished. Tmp file is " + tmpPath + ".\nNum of candidate itemsets is " + jobOne.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue());
            System.out.println(jobOne.getCounters().findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue());
            long totalNum = jobOne.getCounters().findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue();
            System.out.println("+--------------------+");

            // create and execute job two
            conf.set(DMConsts.TOTAL_BASKET_NUM, Long.toString(totalNum));
            Job jobTwo = getDefaultJob(conf, MR_JOB_NAME_TWO, inputPath, outputPath);
            // set mappper
            jobTwo.setMapperClass(SONMapperTwo.class);
            // set Reducer
            jobTwo.setReducerClass(SONReducerTwo.class);
            if (cmdLine.getOptionValue(CLI_PARAM_R2) != null) {
                jobOne.setNumReduceTasks(Integer.parseInt(cmdLine.getOptionValue(CLI_PARAM_R2)));
            }
            // make sure the ouputpath is not occupied by other files
            HDFSUtil.delFile(outputPath);
            jobTwo.waitForCompletion(true);
            sortRst(outputPath);
            long endTime = System.currentTimeMillis();
            System.out.println("Time Elapse: " + (endTime - startTime));
            System.out.println("Num of candidate itemsets is " + jobTwo.getCounters().findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).getValue());
            HDFSUtil.delFile(tmpPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sortRst(String outputPath) throws IOException {
        final Map<String, Integer> itemsets = new HashMap<String, Integer>();

        AdvFile.loadFileInDelimitLine(HDFSUtil.getInputStream(outputPath), new ILineParser() {
            @Override
            public void parseLine(String line) {
                String[] Item;
                Item = line.split("\t");
                itemsets.put(Item[0], Integer.parseInt(Item[1]));
            }
        });

        Map.Entry<String, Integer>[] sortedRst = AdvAlgo.sortMapByValue(itemsets, true);
        PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
        writer.println(sortedRst.length);
        for (Map.Entry<String, Integer> ItemEntry : sortedRst) {
            writer.println(ItemEntry.getKey() + "(" + ItemEntry.getValue() + ")");
        }
        writer.close();
    }

    private Job getDefaultJob(Configuration conf, String jobName, String inputPath, String outputPath) throws IOException {
        Job job = new Job(conf);
        job.setJobName(jobName);
        job.setJarByClass(SON.class);
        // set input & output
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // set format class
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        // set mapper output key & value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // set reducer output key & value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job;
    }

}
