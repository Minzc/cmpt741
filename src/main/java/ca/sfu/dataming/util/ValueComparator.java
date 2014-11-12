package ca.sfu.dataming.util;

import java.util.Comparator;
import java.util.Map;

public class ValueComparator<K extends Comparable<K>, V extends Comparable<V>> implements Comparator<K> {
	private Map<K,V> base;
	
	public ValueComparator(Map<K, V> base){ this.base = base; }
	
	public int compare(K k1, K k2) {
		int val = base.get(k1).compareTo( base.get(k2) );
		if( val == 0 )
			return k1.compareTo(k2);
		else
			return val;
	}
}
