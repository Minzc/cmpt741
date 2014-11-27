package ca.sfu.dataming.mr;

import ca.sfu.dataming.util.DMConsts;
import ca.sfu.dataming.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @author congzicun
 * @since 2014-11-10 10:49 AM
 */
public class SONMapperOne extends Mapper<LongWritable, Text, Text, NullWritable> {
    double supportThrld = 0.1;
    int totalBskts = 0;
    Map<String, Set<Integer>> candidateItemsets = new HashMap<String, Set<Integer>>();
    Map<String, Pair<String, Integer>> prefixIndx = new HashMap<String, Pair<String, Integer>>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        supportThrld = conf.getFloat(DMConsts.SON_SUPPORT_THRSHLD, 0.01f);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        totalBskts++;
        for (String item : value.toString().split(StringUtil.Split_Sign.SPLIT_SPACE.getSign())) {
            if (!candidateItemsets.containsKey(item)) {
                candidateItemsets.put(item, new HashSet<Integer>());
                prefixIndx.put(item, new Pair<String, Integer>("", Integer.parseInt(item)));
            }
            candidateItemsets.get(item).add((int) key.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        supportThrld = totalBskts * supportThrld;
//        System.out.println("Threshold is " + supportThrld + ". total num is " + totalBskts);

//        System.out.println("Before Pruning: " + candidateItemsets.size());
        candidateItemsets = rmNotFrq(candidateItemsets);
//        System.out.println("After Pruning: " + candidateItemsets.size());


        for (String oneItem : candidateItemsets.keySet()) {
            context.write(new Text(oneItem), NullWritable.get());
        }
//        System.out.println("Finish Output Length one");

        while (!candidateItemsets.isEmpty()) {
            Map<String, Set<Integer>> tmpCandidates = new HashMap<String, Set<Integer>>();
            Map<String, Pair<String, Integer>> tmpPrefixIndx = new HashMap<String, Pair<String, Integer>>();


            Set<String> used = new HashSet<String>();
            for (Map.Entry<String, Set<Integer>> i : candidateItemsets.entrySet()) {
                for (Map.Entry<String, Set<Integer>> j : candidateItemsets.entrySet()) {
                    if (i != j && !used.contains(j.getKey())) {
//                        System.out.println("Start union");
                        String iPrefix = prefixIndx.get(i.getKey()).getFirst();
                        String jPrefix = prefixIndx.get(j.getKey()).getFirst();
                        if (iPrefix.equals(jPrefix)) {
                            int iElement = prefixIndx.get(i.getKey()).getSecond();
                            int jElement = prefixIndx.get(j.getKey()).getSecond();
                            // union two item sets' tid list
                            Set<Integer> unionTids = new HashSet<Integer>(candidateItemsets.get(i.getKey()));
                            unionTids.retainAll(candidateItemsets.get(j.getKey()));
//                            System.out.println();
//                            System.out.println(unionTids.size());
//                            if (unionTids.size() < supportThrld)
//                                continue;


                            // generate the prefix and last element of the new item set
                            Pair<String, Integer> prfxNlst = genNewItemSets(iPrefix, iElement, jElement);

                            String newPrefix = prfxNlst.getFirst();
                            int newLstItem = prfxNlst.getSecond();
                            String newItemSet = newPrefix + StringUtil.DELIMIT_1ST + newLstItem;

//                            System.out.println(newItemSet);
//                            System.out.println(unionTids.size());
                            if (unionTids.size() < supportThrld)
                                continue;

                            // Prune the new item set
                            if (pruneItemSet(newItemSet, candidateItemsets)) {
                                tmpCandidates.put(newItemSet, unionTids);
                                // update tmpIndex
                                tmpPrefixIndx.put(newItemSet, new Pair<String, Integer>(newPrefix, newLstItem));
                                context.write(new Text(newItemSet), NullWritable.get());
                            }

                        }
                    }
                }
                used.add(i.getKey());
                candidateItemsets.put(i.getKey(), new HashSet<Integer>());
            }
//            candidateItemsets.clear();
//            prefixIndx.clear();
            candidateItemsets = tmpCandidates;
            prefixIndx = tmpPrefixIndx;
//            System.out.println("Finish");
        }
        System.out.println("Finish Cleaning UP");

    }

    public static Pair<String, Integer> genNewItemSets(String commPrfix, int iElement, int jElement) {
        String newPrefix = commPrfix;
        if (!commPrfix.isEmpty())
            newPrefix += StringUtil.DELIMIT_1ST;

        int newLstItem;

        if (iElement > jElement) {
            newPrefix += jElement;
            newLstItem = iElement;
        } else {
            newPrefix += iElement;
            newLstItem = jElement;
        }
        return new Pair<String, Integer>(newPrefix, newLstItem);
    }

    private boolean pruneItemSet(String itemSet, Map<String, Set<Integer>> candidateItemsets) {
        String[] itemsets = itemSet.split(StringUtil.STR_DELIMIT_1ST);
        for (int i = 0; i < itemsets.length - 2; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < itemsets.length; j++) {
                if (i != j)
                    sb.append(StringUtil.DELIMIT_1ST).append(itemsets[j]);
            }

            String subItemSet = sb.toString().replaceFirst(StringUtil.STR_DELIMIT_1ST,"");
            if (!candidateItemsets.containsKey(subItemSet) || candidateItemsets.get(subItemSet).size() < supportThrld) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Set<Integer>> rmNotFrq(Map<String, Set<Integer>> candidates) {
        Iterator<Map.Entry<String, Set<Integer>>> it = candidates.entrySet().iterator();
//        System.out.println("=====================");
//        System.out.println("Pruning One Items");
        while (it.hasNext()) {
            Map.Entry<String, Set<Integer>> tmp = it.next();
            System.out.println(tmp.getKey());
            if (tmp.getValue().size() < supportThrld) {
                it.remove();
//                System.out.println("it's removed. value is " + tmp.getValue().size());
            }
        }
//        System.out.println("=====================");
        return candidates;
    }
}
