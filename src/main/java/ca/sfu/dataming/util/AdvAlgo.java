package ca.sfu.dataming.util;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.util.Map.Entry;

public class AdvAlgo {


    /**
     * 将Map按照value排序
     *
     * @param map
     * @param isReverse if true, 从大到小; otherwise 从小到大
     * @return 排好序的<K,V>对
     */
    public static <K, V extends Comparable<? super V>> Entry<K, V>[] sortMapByValue(Map<K, V> map, final boolean isReverse) {
        if (map == null)
            return null;
        @SuppressWarnings("unchecked")
        Entry<K, V>[] entries = map.entrySet().toArray(new Entry[map.size()]);
        Arrays.sort(entries, new Comparator<Entry<K, V>>() {
            @Override
            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
                return isReverse ? o2.getValue().compareTo(o1.getValue()) :
                        o1.getValue().compareTo(o2.getValue());
            }

        });
        return entries;
    }

    /**
     * Variable byte encode
     *
     * @param number Number needed being encoded
     * @return bytes which are encoded from number
     */
    public static byte[] vbEncode(int number) {
        int EIGHT_BIT_FLAG = Integer.valueOf("10000000", 2);
        int LOW_SEVEN_BIT_MAX = Integer.valueOf("01111111", 2);
        Stack<Byte> codeStack = new Stack<Byte>();
        codeStack.push(Bytes.toBytes(number & LOW_SEVEN_BIT_MAX)[3]);
        number >>= 7;
        while (number > 0) {
            codeStack.push(Bytes.toBytes(number & LOW_SEVEN_BIT_MAX | EIGHT_BIT_FLAG)[3]);
            number >>= 7;
        }
        byte[] rets = new byte[codeStack.size()];
        for (int i = 0; i < rets.length; i++) {
            rets[i] = codeStack.pop();
        }
        return rets;
    }

    /**
     * Variable byte decode
     *
     * @param bytes bytes which are needed to be decode
     * @return number decoded from given bytes
     */
    public static int vbDecode(byte[] bytes) {
        int EIGHT_BIT_FLAG = Integer.valueOf("10000000", 2);
        int LOW_SEVEN_BIT_MAX = Integer.valueOf("01111111", 2);
        int ret = 0;
        for (byte aByte : bytes) {
            if ((aByte & EIGHT_BIT_FLAG) != 0) {
                ret += aByte & LOW_SEVEN_BIT_MAX;
                ret <<= 7;
            } else {
                ret += aByte;
            }
        }
        return ret;
    }
}
