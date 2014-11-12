package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.AdvFile;
import ca.sfu.dataming.util.DMConfiguration;
import ca.sfu.dataming.util.ILineParser;
import ca.sfu.dataming.util.StringUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;
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
public class SONMapperTwo extends Mapper<LongWritable, Text, Text, LongWritable>{
    Set<String> cItemSets = new HashSet<String>();
    Map<String, Set<Long>> candidateItemsets = new HashMap<String, Set<Long>>();
    Map<String, Pair<String, String>> prefixIndx = new HashMap<String, Pair<String, String>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String cItemSetsPth = context.getConfiguration().get("c.itemsets.path");
        FileSystem fs = FileSystem.get(DMConfiguration.getInstance());
        AdvFile.loadFileInDelimitLine(fs.open(new Path(cItemSetsPth)), new ILineParser() {
            @Override
            public void parseLine(String line) {
                cItemSets.add(line);
            }
        });
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for (String item : value.toString().split(StringUtil.Split_Sign.SPLIT_SPACE.getSign())) {
            // check if item a frequent item
            if (cItemSets.contains(item)) {
                if(!candidateItemsets.containsKey(item)) {
                    Set<Long> tids = new HashSet<Long>();
                    candidateItemsets.put(item, tids);
                }
                candidateItemsets.get(item).add(key.get());
                prefixIndx.put(item, new Pair<String, String>("", item));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String oneItem : candidateItemsets.keySet()) {
            context.write(new Text(oneItem), new LongWritable(candidateItemsets.get(oneItem).size()));
        }

        while (!candidateItemsets.isEmpty()) {
            Map<String, Set<Long>> tmpCandidates = new HashMap<String, Set<Long>>();
            Map<String, Pair<String, String>> tmpPrefixIndx = new HashMap<String, Pair<String, String>>();


            for (Map.Entry<String, Set<Long>> i : candidateItemsets.entrySet()) {
                for (Map.Entry<String, Set<Long>> j : candidateItemsets.entrySet()) {
                    if (i != j) {
                        String iPrefix = prefixIndx.get(i.getKey()).getFirst();
                        String jPrefix = prefixIndx.get(j.getKey()).getFirst();
                        String iElement = prefixIndx.get(i.getKey()).getSecond();
                        String jElement = prefixIndx.get(j.getKey()).getSecond();
                        if (iPrefix.equals(jPrefix)) {
                            // union two item sets' tid list
                            Set<Long> unionTids = new HashSet<Long>(candidateItemsets.get(i.getKey()));
                            unionTids.addAll(candidateItemsets.get(j.getKey()));

                            // generate the prefix and last element of the new item set
                            Pair<String, String> prfxNlst = SONMapperOne.genNewItemSets(iPrefix, iElement, jElement);

                            String newPrefix = prfxNlst.getFirst() + StringUtil.DELIMIT_1ST;
                            String newLstItem = prfxNlst.getSecond();
                            String newItemSet = newPrefix + StringUtil.DELIMIT_1ST + newLstItem;

                            // Prune the new item set
                            if (cItemSets.contains(newItemSet)) {
                                tmpCandidates.put(newItemSet, unionTids);
                                // update tmpIndex
                                tmpPrefixIndx.put(newItemSet, new Pair<String, String>(newPrefix, newLstItem));
                                context.write(new Text(newItemSet), new LongWritable(unionTids.size()));
                            }

                        }
                    }
                }
            }
            candidateItemsets = tmpCandidates;
            prefixIndx = tmpPrefixIndx;
        }
    }
}
