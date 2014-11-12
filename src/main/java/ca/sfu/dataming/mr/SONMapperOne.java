package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.DMConsts;
import ca.sfu.dataming.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @author congzicun
 * @since 2014-11-10 10:49 AM
 */
public class SONMapperOne extends Mapper<LongWritable, Text, Text, IntWritable> {
    double supportThrld = 0.1;
    int totalBskts = 0;
    Map<String, Set<Long>> candidateItemsets = new HashMap<String, Set<Long>>();
    Map<String, Pair<String, String>> prefixIndx = new HashMap<String, Pair<String, String>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        supportThrld = conf.getFloat(DMConsts.SON_SUPPORT_THRSHLD, 0.1f);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        totalBskts++;
        for (String item : value.toString().split(StringUtil.Split_Sign.SPLIT_SPACE.getSign())) {
            if (!candidateItemsets.containsKey(item)) {
                Set<Long> tids = new HashSet<Long>();
                tids.add(key.get());
                candidateItemsets.put(item, tids);
                prefixIndx.put(item, new Pair<String, String>("", item));
            } else {
                candidateItemsets.get(item).add(key.get());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        candidateItemsets = rmNotFrq(candidateItemsets);
        supportThrld = (int) (totalBskts * supportThrld);

        for (String oneItem : candidateItemsets.keySet()) {
            context.write(new Text(oneItem), new IntWritable(1));
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
                            Pair<String, String> prfxNlst = genNewItemSets(iPrefix, iElement, jElement);

                            String newPrefix = prfxNlst.getFirst() + StringUtil.DELIMIT_1ST;
                            String newLstItem = prfxNlst.getSecond();
                            String newItemSet = newPrefix + StringUtil.DELIMIT_1ST + newLstItem;

                            // Prune the new item set
                            if (pruneItemSet(newItemSet, candidateItemsets)) {
                                tmpCandidates.put(newItemSet, unionTids);
                                // update tmpIndex
                                tmpPrefixIndx.put(newItemSet, new Pair<String, String>(newPrefix, newLstItem));
                                context.write(new Text(newItemSet), new IntWritable(1));
                            }

                        }
                    }
                }
            }
            candidateItemsets = tmpCandidates;
            prefixIndx = tmpPrefixIndx;
        }

    }

    public static Pair<String, String> genNewItemSets(String commPrfix, String iElement, String jElement) {
        String newPrefix = commPrfix;
        if(!commPrfix.isEmpty())
            newPrefix += StringUtil.DELIMIT_1ST;

        String newLstItem;

        if (iElement.compareTo(jElement) > 0) {
            newPrefix += jElement;
            newLstItem = iElement;
        } else {
            newPrefix += iElement;
            newLstItem = jElement;
        }
        return new Pair<String, String>(newPrefix, newLstItem);
    }

    private boolean pruneItemSet(String itemSet, Map<String, Set<Long>> candidateItemsets) {
        String[] itemsets = itemSet.split(StringUtil.STR_DELIMIT_1ST);
        for (int i = 0; i < itemsets.length - 2; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < itemsets.length; j++) {
                if (i != j)
                    sb.append(itemsets[j]);
            }

            if (candidateItemsets.get(sb.toString()).size() < supportThrld) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Set<Long>> rmNotFrq(Map<String, Set<Long>> candidates) {
        Iterator<Map.Entry<String, Set<Long>>> it = candidates.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().size() < supportThrld) {
                it.remove();
            }
        }
        return candidates;
    }
}
