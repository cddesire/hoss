/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.hoss.cache;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;


public class Cache implements ICache<String, Metadata, Float> {
	// maintain object name --- object id + object path + object offset
	private HashMap<String, Metadata> cache = null;
	// maintain object name --- object hotness
	private TreeMap<String, Float> name2Hot = null;
	// maintain object hot --- object name
	// private TreeMap<Float, String> hot2Name = null;

	// cache size
	private int capacity;

	public Cache(int capacity) {
		this.capacity = capacity;
		cache = new HashMap<String, Metadata>(capacity);
		name2Hot = new TreeMap<String, Float>();
		// hot2Name = new TreeMap<Float, String>();
	}

	@Override
	public synchronized Metadata get(String objName, Float hot) {
		Metadata meta = cache.get(objName);
		// update object name to hotness relationship
		float historyHot = name2Hot.get(objName);
		float currentHot = historyHot + hot;
		name2Hot.put(objName, currentHot);
		// update hotness to object name relationship
		// hot2Name.remove(historyHot);
		// hot2Name.put(currentHot, objName);
		return meta;
	}

	@Override
	public synchronized void add(String objName, Metadata hossMetadata,
			Float hot) {
		// objName + objId + path position
		hossMetadata.setObjName(objName);
		cache.put(objName, hossMetadata);
		name2Hot.put(objName, hot);
		// hot2Name.put(hot, objName);
	}

	public int size() {
		return cache.size();
	}

	public boolean isFull() {
		return cache.size() >= capacity;
	}

	@Override
	public boolean exist(String key) {
		return cache.containsKey(key);
	}

	/**
	 * get the object name with least hotness
	 * 
	 * @return
	 */
	private synchronized String getLeastHotKey() {
		String keyMin = null;
		float hotMin = Float.MAX_VALUE;
		for (Entry<String, Float> entry : name2Hot.entrySet()) {
			float curHot = entry.getValue();
			if (curHot < hotMin) {
				hotMin = curHot;
				keyMin = entry.getKey();
			}
		}
		return keyMin;
	}

	public synchronized float getLeastHot() {
		float hotMin = Float.MAX_VALUE;
		for (Entry<String, Float> entry : name2Hot.entrySet()) {
			float curHot = entry.getValue();
			if (curHot < hotMin) {
				hotMin = curHot;
			}
		}
		return hotMin;
	}

	/**
	 * remove least hot object metadata
	 */
	@Override
	public synchronized Metadata removeLeast() {
		// remove least hot metadata
		String objName = getLeastHotKey();
		Metadata metadata = null;
		if(objName != null){
		   name2Hot.remove(objName);
		   cache.remove(objName);
		}
		return metadata;
	}

	/**
	 * remove the object from cache
	 * 
	 * @param key
	 * @return
	 */
	public synchronized Metadata remove(String objName) {
		Metadata metadata = null;
		if (cache.containsKey(objName)) {
			metadata = cache.remove(objName);
			name2Hot.remove(objName);
			// hot2Name.remove(hot);
		}
		return metadata;
	}

	/**
	 * only for warm cache
	 * 
	 * @param size
	 */
	public synchronized void freeSpace(int size) {
		for (int i = 0; i < size; i++) {
			// Entry<Float, String> least = hot2Name.pollFirstEntry();
			String objName = getLeastHotKey();
			if (objName != null) {
				name2Hot.remove(objName);
				cache.remove(objName);
			}
		}
	}

	public float getHot(String objName) {
		return name2Hot.get(objName);
	}

	public synchronized void ageCache(float hotMin) {
		TreeMap<String, Float> name2HotTmp = new TreeMap<String, Float>();
		for (Entry<String, Float> entry : name2Hot.entrySet()) {
			name2HotTmp.put(entry.getKey(), entry.getValue() - hotMin);
		}
		name2Hot.clear();
		name2Hot = name2HotTmp;
	}

	@Override
	public String toString() {
		StringBuilder cache = new StringBuilder();
		for (Entry<String, Float> entry : name2Hot.entrySet()) {
			cache.append(entry.getKey() + " " + entry.getValue() + "\n");
		}
		return cache.toString();
	}
	
	public TreeSet<HotObject> listHot(){
		TreeSet<HotObject> hotSet= new TreeSet<HotObject>();
		for (Entry<String, Float> entry : name2Hot.entrySet()) {
			hotSet.add(new HotObject(entry.getValue(), entry.getKey()));
		}
		return hotSet;
	}
	
}
