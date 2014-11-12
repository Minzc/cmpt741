package ca.sfu.dataming.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author congzicun
 * @since 2014-11-11 11:32 AM
 */
public class SONReducerTwo extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long counter = 0;
        for (LongWritable value : values) {
            counter += value.get();
        }
        context.write(key, new LongWritable(counter));
    }
}
