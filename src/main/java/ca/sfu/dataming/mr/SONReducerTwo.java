package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.DMConsts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author congzicun
 * @since 2014-11-11 11:32 AM
 */
public class SONReducerTwo extends Reducer<Text, IntWritable, Text, IntWritable> {

    double supportThrld;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        supportThrld = conf.getFloat(DMConsts.SON_SUPPORT_THRSHLD, 0.01f);
        long totalLines = context.getConfiguration().getLong(DMConsts.TOTAL_BASKET_NUM, 0);
        if(totalLines == 0)
            throw new InterruptedException("Total number of baskets can not be zero!");

        supportThrld = (int) (supportThrld * totalLines);
        System.out.println("supportThrld is " + supportThrld);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        for (IntWritable value : values) {
            counter += value.get();
        }
        if (counter >= supportThrld)
            context.write(key, new IntWritable(counter));
    }
}
