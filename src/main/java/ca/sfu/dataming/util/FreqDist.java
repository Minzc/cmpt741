package ca.sfu.dataming.util;

import java.util.*;

/**
 * A generic class for counting frequency distribution
 * @author Yabo
 * @param <K>	key type
 * @param <V>	value type
 */
@SuppressWarnings("serial")
public class FreqDist<K extends Comparable<K>> extends HashMap<K, Integer> {
	private int total = 0;
	
	public FreqDist(){}
	
	public void add( K key, int val ){
		Integer oldVal = containsKey(key)? get(key): new Integer(0);
		put(key, oldVal + val );
		total += val;
	}
	public void incr( K key ){ 
		add(key, 1);
	}
	public void addAll( Collection<K> c){ 
		for(K k:c) incr(k);
	}
	
	//Return sorted key set, decreasing order
	public List<K> sortedKeys(){
        ValueComparator<K, Integer> vComp =  new ValueComparator<K, Integer>( this );
        List<K> list = new LinkedList<K>(this.keySet());
        Collections.sort(list, Collections.reverseOrder(vComp));
        return list;
	}
	
	public K uniqMax(){	//return the key with max val if it is unique; otherwise null;
		if( isEmpty() ) return null;
		List<K> kSet = sortedKeys();
		Iterator<K> it = kSet.iterator();
		K firstKey = it.next();
		if( size() == 1 ) 
			return firstKey;
		else
			return get(firstKey) > get(it.next())? firstKey: null;		
	}
	
	//Get frequency distribution for a key
	public float getDist( K k ){ return (float) getCount(k)/total;}
	
	public int getCount(K k){ return containsKey(k)?get(k):0; }
	
	/**
	 * Get the distribution with additive smoothing 
	 * Reference: http://en.wikipedia.org/wiki/Additive_smoothing
	 * @param k the key 
	 * @param sizeOfCategory the size of distinct categories
	 * @param smoothFactor in range [0,1]; 0 means no smoothing and 1 means 
	 * 		smoothing by a uniform probability
	 * @return ( count(k) + smoothFactor ) / ( total + smoothFactor*sizeOfCategory)
	 */
	public float getAdditiveSmoothedDist( K k, int sizeOfCategory, float smoothFactor ){
		return ( get(k) + smoothFactor ) / (total + (smoothFactor*sizeOfCategory)); 
	}
	
}
