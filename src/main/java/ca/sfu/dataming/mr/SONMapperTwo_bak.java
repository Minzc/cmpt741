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
public class SONMapperTwo_bak extends Mapper<LongWritable, Text, Text, IntWritable> {
    Set<String> cItemSets = new HashSet<String>();
    Map<String, Set<Integer>> candidateItemsets = new HashMap<String, Set<Integer>>();
    Map<String, Pair<String, Integer>> prefixIndx = new HashMap<String, Pair<String, Integer>>();
    int lnCounter = 0;

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
        lnCounter ++;
        for (String item : value.toString().split(StringUtil.Split_Sign.SPLIT_SPACE.getSign())) {
            // check if item a frequent item
            if (cItemSets.contains(item)) {
                if (!candidateItemsets.containsKey(item)) {
                    Set<Integer> tids = new HashSet<Integer>();
                    candidateItemsets.put(item, tids);
                }
                candidateItemsets.get(item).add((int) key.get());
                prefixIndx.put(item, new Pair<String, Integer>("", Integer.parseInt(item)));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String oneItem : candidateItemsets.keySet()) {
            context.write(new Text(oneItem), new IntWritable(candidateItemsets.get(oneItem).size()));
        }

        while (!candidateItemsets.isEmpty()) {
            Map<String, Set<Integer>> tmpCandidates = new HashMap<String, Set<Integer>>();
            Map<String, Pair<String, Integer>> tmpPrefixIndx = new HashMap<String, Pair<String, Integer>>();


            Set<String> used = new HashSet<String>();
            for (Map.Entry<String, Set<Integer>> i : candidateItemsets.entrySet()) {
                for (Map.Entry<String, Set<Integer>> j : candidateItemsets.entrySet()) {
                    if (i != j && !used.contains(j.getKey())) {
                        String iPrefix = prefixIndx.get(i.getKey()).getFirst();
                        String jPrefix = prefixIndx.get(j.getKey()).getFirst();
                        int iElement = prefixIndx.get(i.getKey()).getSecond();
                        int jElement = prefixIndx.get(j.getKey()).getSecond();
                        if (iPrefix.equals(jPrefix)) {
                            // union two item sets' tid list
                            Set<Integer> unionTids = new HashSet<Integer>(candidateItemsets.get(i.getKey()));
                            unionTids.retainAll(candidateItemsets.get(j.getKey()));

                            // generate the prefix and last element of the new item set
                            Pair<String, Integer> prfxNlst = SONMapperOne.genNewItemSets(iPrefix, iElement, jElement);

                            String newPrefix = prfxNlst.getFirst() ;
                            int newLstItem = prfxNlst.getSecond();
                            String newItemSet = newPrefix + StringUtil.DELIMIT_1ST + newLstItem;


                            // Prune the new item set
                            if (cItemSets.contains(newItemSet)) {
                                tmpCandidates.put(newItemSet, unionTids);
                                // update tmpIndex
                                tmpPrefixIndx.put(newItemSet, new Pair<String, Integer>(newPrefix, newLstItem));
                                context.write(new Text(newItemSet), new IntWritable(unionTids.size()));
//                                if(newItemSet.split(StringUtil.STR_DELIMIT_1ST).length > 1)
//                                    System.out.println("out!");
                            }
//                            if(newItemSet.split(StringUtil.STR_DELIMIT_1ST).length > 1)
//                                System.out.println(newItemSet);

                        }
                    }
                }
                used.add(i.getKey());
            }
            candidateItemsets = tmpCandidates;
            prefixIndx = tmpPrefixIndx;
        }
    }
}
