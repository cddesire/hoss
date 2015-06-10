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
package org.apache.hadoop.hdfs.hoss.db;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.hoss.bloomfilter.HosBloomFilter;
import org.apache.hadoop.hdfs.hoss.cache.HossCache;
import org.apache.hadoop.hdfs.hoss.cache.HotObject;
import org.apache.hadoop.hdfs.hoss.cache.Metadata;
import org.apache.hadoop.hdfs.hoss.util.ByteUtil;
import org.apache.hadoop.hdfs.hoss.util.FileUtil;
import org.apache.hadoop.hdfs.hoss.util.HDFSUtil;

public class HosMetaData {

	private static final Log LOG = LogFactory.getLog(HosMetaData.class);

	private static FileSystem fs = HDFSUtil.getFileSystem();

	public static final String HOSSDIR = "meta";

	public static final String IDSFILE = "ids";

	public static final String INDEXFILE = "hoss.spi";

	public static final String DATAFILE = "hoss.spl";

	public static final String BLOOMFILTER = "bloomfilter";

	public static final String PATHFILE = "pathposition";

	public static final String HOTFILE = "hotness";

	private static final int BUFFERSIZE = 3000000;

	private static final int WARMCAPACITY = 4000;

	private static final int HOTCAPACITY = 800;

	private static HossCache hossCache = null;

	private static boolean disablecache = false;

	private ObjectsMap objectsMap = null;

	private AtomicLong currentId = null;

	// deleted ids in hoss.spl
	private TreeSet<Long> ids = null;

	// deleted object names in hoss.spl
	private TreeSet<String> deletedObjs = new TreeSet<String>();

	private ObjectId objId = null;

	private HosBloomFilter hosBloomFilter = null;

	private PathStore ps = null;

	private HotStore hs = null;

	// for test cache hit ratio
	// public static long requests = 0L;

	// public static long hits = 0L;

	private ReentrantReadWriteLock hosLock = new ReentrantReadWriteLock();

	public HosMetaData() {
		Configuration conf = new Configuration();
		String hosDir = conf.get("hoss.meta.dir", HOSSDIR);
		int hotCapacity = conf.getInt("hoss.hotCapacity", HOTCAPACITY);
		int warmCapacity = conf.getInt("hoss.warmCapacity", WARMCAPACITY);
		LOG.info("hoss meta directory: " + conf.get("hoss.meta.dir"));
		LOG.info("hoss hot cache capacity:  " + conf.get("hoss.hotCapacity"));
		LOG.info("hoss warm cache capacity:  " + conf.get("hoss.warmCapacity"));
		initialize(hosDir, warmCapacity, hotCapacity);
		// this.addShutdownHook();
	}

	private void initialize(String metaDir, int warmCapacity, int hotCapacity) {
		objectsMap = new ObjectsMap(new File(metaDir));
		objId = new ObjectId();
		currentId = new AtomicLong(objId.getCurrentId());
		ids = objId.getDeletedIDSet();
		LOG.info("current id: " + currentId);
		hosBloomFilter = new HosBloomFilter();
		ps = new PathStore();
		hs = new HotStore();
		if (!disablecache) {
			hossCache = new HossCache(warmCapacity, hotCapacity);
		}
	}

	public void saveMetaData() throws IOException {
		// first save the object id
		objId.saveDeletedIDs(currentId.get(), ids);

		// second save the object map
		objectsMap.compact(hosBloomFilter);

		// third save the hos bloom filter
		hosBloomFilter.close();

		ps.close();

		hs.close();
	}

	private long nextObjectId() {
		long id = 0;
		if (ids.size() != 0) {
			id = ids.first();
			ids.remove(id);
		} else {
			id = currentId.getAndIncrement();
		}
		return id;
	}

	/**
	 * set object name and object id
	 * 
	 * @param objName
	 * @throws IOException
	 */
	public PathPosition setNameId(String objName, long objId) {
		hosLock.writeLock().lock();
		PathPosition pp = null;
		try {
			objectsMap.put(objName, objId);
			pp = ps.put(objId);
			hosBloomFilter.add(objName);
		} finally {
			hosLock.writeLock().unlock();
		}
		return pp;
	}

	/**
	 * 
	 * @param objName
	 * @throws IOException
	 */
	public PathPosition put(String objName) throws IOException {
		boolean exist = hosBloomFilter.contain(objName);
		PathPosition pp = null;
		if (exist) {
			LOG.warn("object " + objName + " has existed in Hos ");
			return null;
		} else {
			hosLock.writeLock().lock();
			try {
				long id = this.nextObjectId();
				if (id == -1) {
					LOG.warn("object name: " + objName + "  id: " + id);
					return null;
				}
				// LOG.info("put objName: "+ objName + "  id: " + id);
				if (objectsMap.memSize() > BUFFERSIZE) {
					objectsMap.append(deletedObjs);
					deletedObjs.clear();
				}
				objectsMap.put(objName, id);
				pp = ps.put(id);
				// set object size -1L. we will rest its size
				// after finishing putting.
				setObjectSize(id, -1L);
				// add hos filter
				hosBloomFilter.add(objName);
			} finally {
				hosLock.writeLock().unlock();
			}
		}
		return pp;
	}

	private synchronized void setObjectSize(long objId, long size) {
		// write create time + last access time + size
		long current = System.currentTimeMillis();
		hs.put(objId, current, current, size);
	}

	/**
	 * set object id and pathid 
	 * @param objId
	 * @param pathId
	 * @param offset
	 */
	public synchronized void updatePathPos(long objId, long pathId, long offset) {
		ps.put(objId, pathId, offset);
	}

	public boolean exist(String objName) {
		return hosBloomFilter.contain(objName);
	}
	/**
	 * objId is deleted and no reused
	 * 
	 * @param objId
	 * @return
	 */
	public boolean exist(long objId) {
		return ids.contains(objId);
	}

	/**
	 * 
	 * @param objName
	 * @return the to-be-accessed object id currentId
	 * @throws IOException
	 */
	public long getId(String objName) throws IOException {
		long objId = -1L;
		if (exist(objName)) {
			hosLock.readLock().lock();
			try {
				objId = objectsMap.get(objName);
			} finally {
				hosLock.readLock().unlock();
			}
		} else {
			LOG.warn("object " + objName + " does not exist in Hos.");
		}
		return objId;
	}

	/**
	 * 
	 * @param objName
	 * @return
	 * @throws IOException
	 */
	public PathPosition getPathPosition(String objName) throws IOException {
		PathPosition pp = null;
		long objId = getId(objName);
		if (objId < 0) {
			LOG.warn("object " + objName + " does not exist in Hos.");
			return null;
		}
		// requests++;
		if (!disablecache) {
			if (hossCache.exist(objName)) {// read from flash
				// hits++;
				float hotness = hs.hot(objId);
				pp = hossCache.hit(objName, hotness).getPathPosition();
			} else {// read from flash
				pp = getPathPosition(objId);
				float hotness = getHotness(objId, pp);
				hossCache.addCache(objName, new Metadata(objId, pp, objName),
						hotness);
			}
		} else {
			pp = getPathPosition(objId);
		}
		return pp;
	}

	private float getHotness(long objId, PathPosition pp) {
		// get from hot store
		long size = hs.getObjectSizeMB(objId);
		float hotness = 0f;
		if (size < 0) {
			long bytes = getObjectSizeBytes(objId);
			hotness = hs.firstHot(objId, bytes);
		} else {
			hotness = hs.hot(objId);
		}
		return hotness;
	}

	/**
	 * get from hoss system
	 * 
	 * @param objId
	 * @return object size(unit:bytes), if object has combined return -1;
	 */
	public long getObjectSizeBytes(long objId) {
		long bytes = -1L;
		PathPosition pp = ps.get(objId);
		if (pp.getOffset() == 0) {
			Path f = new Path(pp.getPath());
			FileStatus status = null;
			try {
				status = fs.getFileStatus(f);
			} catch (IOException e) {
				LOG.error("get object size, object id is " + objId);
			}
			// unit:bytes
			bytes = status.getLen();
		}
		return bytes;
	}

	private boolean isSmall(long bytes) {
		bytes = bytes >>> 20;
		return bytes < 16 ? true : false;
	}

	/**
	 * 
	 * @return the small objects set (objectId + size bytes)
	 */
	public Map<Long, Integer> smallObjects() {
		Map<Long, Integer> smallObjectsSet = new HashMap<Long, Integer>();
		long curId = this.currentId.get();
		for (long i = 1L; i < curId; i++) {
			//object is combined if bytes is -1, 
			long bytes = getObjectSizeBytes(i);
			if (bytes != -1L && isSmall(bytes)) {
				smallObjectsSet.put(i, (int) bytes);
			}
		}
		return smallObjectsSet;
	}

	/**
	 * 
	 * @param objName
	 * @return
	 * @throws IOException
	 */
	public PathPosition getPathPosition(long objId) {
		PathPosition pp = ps.get(objId);
		return pp;
	}

	/**
	 * 
	 * @param objName
	 * @return the to-be-deleted object id, if id < 0, object is not existed.
	 * @throws IOException
	 */
	public long delete(String objName) throws IOException {
		boolean exist = hosBloomFilter.contain(objName);
		if (!exist) {
			LOG.warn("object " + objName + " does not exit.");
			return -1;
		}
		hosLock.writeLock().lock();
		long id = -1;
		try {
			deletedObjs.add(objName);
			id = objectsMap.delete(objName);
			if (id > -1) {
				ids.add(id);
			}
			//delete object from cache
			hossCache.remove(objName);
			//delete object from bloom filter
			hosBloomFilter.remove(objName);
		} finally {
			hosLock.writeLock().unlock();
		}
		return id;
	}

	/**
	 * list the object name and object id in hoss
	 * 
	 * @return
	 * @throws IOException
	 */
	public Map<String, Long> listObjects() {
		Map<String, Long> objects = null;
		try {
			objects = objectsMap.list(hosBloomFilter);
		} catch (IOException e) {
			LOG.error("get all the objects IOException");
		}
		return objects;
	}

	public List<HotObject> topHotObject(int top) {
		return hossCache.topHot(top);
	}

	/**********************************************************************/

	/**
	 * allocate the global unique object
	 * 
	 * @author desire
	 * 
	 */
	class ObjectId {

		private long currentId = 1;

		private TreeSet<Long> deletedIDSet = new TreeSet<Long>();
		// the file to store deleted ids for recycle use
		private File deFile = null;

		public ObjectId() {
			File deletedIdFile = new File(new File(HOSSDIR), IDSFILE);
			deFile = deletedIdFile;
			if (!deletedIdFile.exists()) {
				boolean success = false;
				try {
					success = FileUtil.createEmpty(deletedIdFile);
				} catch (IOException e) {
					LOG.error("create deleted id file fail.");
				}
				if (!success)
					try {
						throw new IOException("Could not create log directory "
								+ deletedIdFile.getAbsolutePath());
					} catch (IOException e) {
						LOG.error("Could not create log directory ");
					}
			}
			initDeletedIds();
		}

		/**
		 * read deleted ids from disk.
		 */
		private void initDeletedIds() {
			byte[] array = null;
			try {
				array = FileUtil.readBytesFromFile(deFile);
			} catch (IOException e) {
				LOG.error("initilize deleted ids fail", e);
			}
			if (array != null && array.length != 0) {
				long[] ids = ByteUtil.toLongArray(array);
				// init the current id
				this.setCurrentId(ids[0]);
				for (int i = 1; i < ids.length; i++) {
					deletedIDSet.add(ids[i]);
				}
			}
		}

		public long getCurrentId() {
			return currentId;
		}

		public void setCurrentId(long currentId) {
			this.currentId = currentId;
		}

		public TreeSet<Long> getDeletedIDSet() {
			return deletedIDSet;
		}

		public void setDeletedIDSet(TreeSet<Long> deletedIDSet) {
			this.deletedIDSet = deletedIDSet;
		}

		public void saveDeletedIDs(long currentId, Set<Long> deletedIds)
				throws IOException {
			// clear file
			FileUtil.clearFile(deFile);
			// current id + deleted ids
			int length = deletedIds.size() + 1;
			long[] ids = new long[length];
			ids[0] = currentId;
			int i = 1;
			for (Long id : deletedIds) {
				ids[i++] = id;
			}
			byte[] bArray = ByteUtil.toByteArray(ids);
			FileUtil.writeBytesToFile(deFile, bArray);
		}
	}

}
