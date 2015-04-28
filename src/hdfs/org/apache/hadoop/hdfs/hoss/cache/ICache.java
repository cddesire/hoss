package org.apache.hadoop.hdfs.hoss.cache;

public interface ICache<K,V,D> {
	V get(K key, D hot);

	void add(K key, V value, D hot);
	
	boolean exist(K key);
	
	V removeLeast();
	
}
