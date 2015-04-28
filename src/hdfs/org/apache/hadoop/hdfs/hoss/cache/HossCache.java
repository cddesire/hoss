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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HossCache {
	private static final Log LOG = LogFactory.getLog(HossCache.class);
	
	private static Cache warmCache = null;
	
	private static Cache hotCache = null;
	
	public HossCache(int warmCapacity, int hotCapacity) {
		warmCache = new Cache(warmCapacity);
		hotCache = new Cache(hotCapacity);
	}

	public synchronized boolean exist(String objName) {
		return warmCache.exist(objName) || hotCache.exist(objName);
	}

	public synchronized void addCache(String objName, Metadata metadata,
			float hot) {
		if (warmCache.isFull()) {
			warmCache.removeLeast();
		}
		warmCache.add(objName, metadata, hot);
	}

	public synchronized Metadata hit(String objName, float hot) {
		Metadata metadata = null;
		if (warmCache.exist(objName)) {// hit in warm Cache
			metadata = getWarmCache(objName, hot);
			/*
			 * LOG.info("hit warm cache: objectName: " + objName + " hotness: "
			 * + hot + " " + metadata);
			 */
		} else {// hit in hot Cache
			metadata = getHotCache(objName, hot);
			/*
			 * LOG.info("hit hot cache: objectName: " + objName + " hotness: " +
			 * hot + " " + metadata);
			 */
		}
		return metadata;
	}

	private synchronized Metadata getWarmCache(String objName, float hot) {
		Metadata metadata = null;
		metadata = warmCache.get(objName, hot);
		float historyHot = warmCache.getHot(objName);
		float currentHot = historyHot + hot;
		if (hotCache.isFull()) {
			// warm cache is full, free some space for metadata from hot cache
			if (warmCache.isFull()) {
				int freeWarmSpace = warmCache.size() / 10 + 1;
				warmCache.freeSpace(freeWarmSpace);
			}
			// migrate meatadata with the lowest hotness from hot cache to warm
			// cache

			float hInhot = hotCache.getLeastHot();
			Metadata metaInHot = hotCache.removeLeast();
			// hotCache.ageCache(hot);
			if (metaInHot != null) {
				warmCache.add(metaInHot.getObjName(), metaInHot, hInhot);
				LOG.info("migrate object: " + metaInHot.getObjName()
						+ " hotness: " + hInhot);
			}
		}
		hotCache.add(objName, metadata, currentHot);
		warmCache.remove(objName);
		return metadata;
	}

	private Metadata getHotCache(String objName, float hot) {
		Metadata metadata = null;
		metadata = hotCache.get(objName, hot);
		return metadata;
	}

}
