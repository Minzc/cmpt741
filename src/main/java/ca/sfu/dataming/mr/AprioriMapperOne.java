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
public class AprioriMapperOne extends Mapper<LongWritable, Text, Text, NullWritable> {
    double supportThrld = 0.1;
    int totalBskts = 0;
    List<String> transactions = new LinkedList<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        supportThrld = conf.getFloat(DMConsts.SON_SUPPORT_THRSHLD, 0.01f);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        totalBskts++;
        transactions.add(value.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            Apriori apriori = new Apriori(transactions, supportThrld);
            apriori.go(context);
        } catch (Exception e) {
            e.printStackTrace();
        }


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
