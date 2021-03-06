package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author congzicun
 * @since 2014-11-11 11:32 AM
 */
public class SONMapperTwo extends Mapper<LongWritable, Text, Text, IntWritable> {
    Set<String> cItemSets = new HashSet<String>();
    int lnCounter = 0;
    FreqDist<String> counter = new FreqDist<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String cItemSetsPth = context.getConfiguration().get(DMConsts.C_ITEMSETS_PATH);
        FileSystem fs = FileSystem.get(DMConfiguration.getInstance());
        FileStatus[] status_list = fs.listStatus(new Path(cItemSetsPth));
        for (FileStatus status : status_list) {
            if (status.getPath().toString().contains("_"))
                continue;
            AdvFile.loadFileInDelimitLine(fs.open(status.getPath()), new ILineParser() {
                @Override
                public void parseLine(String line) {
                    cItemSets.add(line);
                }
            });
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        lnCounter++;
        Set<String> candidateItemsets = new HashSet<String>();
        Map<String, Pair<String, Integer>> prefixIndx = new HashMap<String, Pair<String, Integer>>();
        for (String item : value.toString().split(StringUtil.Split_Sign.SPLIT_SPACE.getSign())) {
            // check if item a frequent item
            if (cItemSets.contains(item)) {
                counter.incr(item);
                candidateItemsets.add(item);
                prefixIndx.put(item, new Pair<String, Integer>("", Integer.parseInt(item)));
            }
        }


        while (!candidateItemsets.isEmpty()) {
            Set<String> tmpCandidates = new HashSet<String>();
            Map<String, Pair<String, Integer>> tmpPrefixIndx = new HashMap<String, Pair<String, Integer>>();


            Set<String> used = new HashSet<String>();
            for (String i : candidateItemsets) {
                for (String j : candidateItemsets) {
                    if (!i.equals(j) && !used.contains(j)) {
                        String iPrefix = prefixIndx.get(i).getFirst();
                        String jPrefix = prefixIndx.get(j).getFirst();
                        int iElement = prefixIndx.get(i).getSecond();
                        int jElement = prefixIndx.get(j).getSecond();
                        if (iPrefix.equals(jPrefix)) {

                            // generate the prefix and last element of the new item set
                            Pair<String, Integer> prfxNlst = SONMapperOne.genNewItemSets(iPrefix, iElement, jElement);

                            String newPrefix = prfxNlst.getFirst();
                            int newLstItem = prfxNlst.getSecond();
                            String newItemSet = newPrefix + StringUtil.DELIMIT_1ST + newLstItem;


                            // Prune the new item set
                            if (cItemSets.contains(newItemSet)) {
                                tmpCandidates.add(newItemSet);
                                // update tmpIndex
                                tmpPrefixIndx.put(newItemSet, new Pair<String, Integer>(newPrefix, newLstItem));
                                counter.incr(newItemSet);
                            }

                        }
                    }
                }
                used.add(i);
            }
            candidateItemsets = tmpCandidates;
            prefixIndx = tmpPrefixIndx;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (String itemset : counter.keySet()) {
            context.write(new Text(itemset), new IntWritable(counter.get(itemset)));
        }
    }
}
