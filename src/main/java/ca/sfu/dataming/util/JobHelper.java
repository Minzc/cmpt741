package ca.sfu.dataming.util;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.Map;

/**
 * @author congzicun
 * @since 2014-11-02 3:15 PM
 */
public class JobHelper {

    /**
     * Initialize and submit a mr job to hadoop cluster. Waiting to the job to finish.
     *
     * @param jobClass
     * @param jobName
     * @param inputPaths input paths that are separated by comma
     * @param outputPath
     * @param params     parameters that are defined in dujob and needed to be transited to mr job
     * @return true if the job succeeded.
     */
    public static boolean run(Class<? extends MRJob> jobClass, String jobName, String inputPaths, String outputPath, Map<String, String> params) {
        try {
            MRJob mrJob = ReflectionUtils.newInstance(jobClass, DMConfiguration.getInstance());
            Job job = mrJob.createJob(new String[]{
                    inputPaths, outputPath
            });

            if (params != null && !params.isEmpty()) {
                for (Map.Entry<String, String> entry : params.entrySet())
                    job.getConfiguration().set(entry.getKey(), entry.getValue());
            }

            String jobID = runJobAndGetJobID(job, true);
            boolean status = (jobID != null);
            if (!status) {
                System.err.print(jobName + " Failed");
            }
            return status;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Initialize and submit a job to MR cluster. Wait for the job to finish.
     *
     * @param jobClass
     * @param jobName
     * @param inputPaths input paths that are separated by comma
     * @param outputPath
     * @param params     parameters that are defined in dujob and needed to be transited to mr job
     * @return job object if the job succeed. null if the job failed
     */
    public static Job runAndGetJob(Class<? extends MRJob> jobClass, String jobName, String inputPaths, String outputPath, Map<String, String> params) {
        try {
            MRJob mrJob = ReflectionUtils.newInstance(jobClass, DMConfiguration.getInstance());
            Job job = mrJob.createJob(new String[]{
                    inputPaths, outputPath
            });

            if (params != null && !params.isEmpty()) {
                for (Map.Entry<String, String> entry : params.entrySet())
                    job.getConfiguration().set(entry.getKey(), entry.getValue());
            }
            if (job.waitForCompletion(true)) {
                return job;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String runJobAndGetJobID(Job job, boolean verbose) throws Exception {
        if (job.waitForCompletion(verbose)) {
            return job.getJobID().toString();
        }
        return null;
    }

    public static interface MRJob {
        Job createJob(String[] args) throws IOException;
    }
}

