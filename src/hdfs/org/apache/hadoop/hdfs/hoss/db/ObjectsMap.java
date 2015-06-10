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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.hoss.db.HosMetaData;
import org.apache.hadoop.hdfs.hoss.bloomfilter.HosBloomFilter;
import org.apache.hadoop.hdfs.hoss.meta.CompressionType;
import org.apache.hadoop.hdfs.hoss.meta.Hos;
import org.apache.hadoop.hdfs.hoss.meta.HosReader;
import org.apache.hadoop.hdfs.hoss.meta.HosWriter;
import org.apache.hadoop.hdfs.hoss.util.ByteUtil;
import org.apache.hadoop.hdfs.hoss.util.FileUtil;


public class ObjectsMap {
	private static final Log LOG = LogFactory.getLog(ObjectsMap.class);

	private HosReader reader = null;

	private ConcurrentHashMap<String, Long> memMap = new ConcurrentHashMap<String, Long>();
	
	//recycle use the deleted ids 
	private List<byte[]> deletedIds = new ArrayList<byte[]>();

	//ConcurrentSkipListSet<String> deletedObjSet = new ConcurrentSkipListSet<String>();
	
	private File dir = null;

	private static final int BLOCKSIZE = 4 * 1024;

	public ObjectsMap(File dir) {
		this.dir = dir;
		try {
			this.initDir();
		} catch (IOException e1) {
			LOG.error("initilize hos meta data directory fail", e1);
		}
		File index = new File(dir, HosMetaData.INDEXFILE);
		try {
			reader = Hos.open(index);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//this.addShutdownHook();
	}

	private void initDir() throws IOException {
		if (!dir.exists()) {
			boolean success = dir.mkdirs();
			if (!success)
				throw new IOException("Could not create hos meta data directory "
						+ dir.getAbsolutePath());
		}
		File index = new File(dir, HosMetaData.INDEXFILE);
		if (!index.exists()) {
			HosWriter writer = Hos.createNew(index, CompressionType.SNAPPY,
					BLOCKSIZE);
			writer.flush();
			writer.writeHash();
			writer.close();
		}

	}
	
	public int memSize() {
		return memMap.size();
	}
	

	/**
	 * write objName and objId to log file.
	 * 
	 * @param objName
	 * @param objId
	 * @throws IOException
	 */
	private void putLog(String objName, long objId, HosWriter writer)
			throws IOException {
		byte[] key = objName.getBytes(Charset.forName("UTF-8"));
		byte[] value = ByteBuffer.allocate(8).putLong(objId).array();
		writer.put(key, value);
	}

	private void putAll(Map<String, Long> objectsMap, HosWriter writer)
			throws IOException {
		if (objectsMap.size() != 0) {
			for (Map.Entry<String, Long> entry : objectsMap.entrySet()) {
				putLog(entry.getKey(), entry.getValue(), writer);
			}
		}
	}

	/**
	 * put the object name and object id to memory buffer.
	 * 
	 * @param objName
	 * @param objId
	 */
	public void put(String objName, long objId) {
		memMap.put(objName, objId);
	}

	private long getMem(String objName) {
		long objId = -1;
		if (memMap.containsKey(objName)) {
			objId = memMap.get(objName);
		}
		return objId;
	}

	/**
	 * 
	 * @param objName
	 * @return object id if not exists, return -1
	 * @throws IOException
	 */
	public long get(String objName) throws IOException {
		long objId = -1;
		// get object id from memory map
		if ((objId = getMem(objName)) != -1) {
			return objId;
		}
		// get from the external memory
		byte[] key = objName.getBytes(Charset.forName("UTF-8"));
		byte[] value = reader.getAsByteArray(key);
		if (value != null && value.length == 8) {
			objId = ByteBuffer.wrap(value).getLong();
		}
		return objId;
	}

	private long deleteMem(String objName) {
		long id = -1;
		if (memMap.containsKey(objName)) {
			id = memMap.remove(objName);
		}
		return id;
	}

	/**
	 * delete the object and object id
	 * 
	 * @param objName
	 * @throws IOException
	 */
	public long delete(String objName) throws IOException {
		long id = deleteMem(objName);
		// if not exists in memory map, add deleted set.
		if (id == -1) {
			//need not get id from external memory
			//compact will cycle the object id
			id = -2;
		}
		return id;
	}

	/**
	 * list the object name and object id 
	 * @param hosBloomFilter
	 * @return the pair of object name and id
	 * @throws IOException
	 */
	public Map<String, Long> list(HosBloomFilter hosBloomFilter) throws IOException {
		Map<String, Long> map = new HashMap<String, Long>();
		// get form the external memory
		for (HosReader.Entry entry : reader) {
			byte[] key = entry.getKey();
			byte[] value = entry.getValue();
			String newKey = new String(key, Charset.forName("UTF-8"));
			//update the key memMap.contains(newKey)
			boolean isChanged = memMap.contains(newKey) || !hosBloomFilter.contain(newKey);
			if (!(isChanged)) {
				map.put(newKey, ByteBuffer
						.wrap(value).getLong());
			}
		}
		for (Map.Entry<String, Long> entry : memMap.entrySet()) {
			map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

	/**
	 * the object name is updated or deleted.
	 * 
	 * @param key
	 * @return
	 */
	private boolean isChanged(byte[] key, byte[] value, HosBloomFilter hosBloomFilter) {
		String changedKey = new String(key, Charset.forName("UTF-8"));
		if (!hosBloomFilter.contain(changedKey)) {
			deletedIds.add(value);
			return true;
		} else if (memMap.contains(changedKey)) {
			return true;
		} else {
			return false;
		}
	}

	public void append(TreeSet<String> deletedObjs) throws IOException {
		long start = System.currentTimeMillis();
		reader.close();
		File index = new File(dir, HosMetaData.INDEXFILE);
		HosWriter writer = Hos.append(index);
		if(deletedObjs.size() != 0) {
			for(String obj: deletedObjs) {
				writer.delete(obj);
			}
		}
		System.out.println("memMap size " + memMap.size());
		//flush memory data to hos.spl
		putAll(memMap, writer);
		memMap.clear();
		//writer.flush();
		writer.writeHash();
		writer.close();
		try {
			reader = Hos.open(index);
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("flush memory data to hos.spl using " + 
		(System.currentTimeMillis() - start) + " ms");
	}
	
	
	public void compact(HosBloomFilter hosBloomFilter) throws IOException {
		if ( memMap.size() != 0){
			this.compact1(hosBloomFilter);
		}
	}
	
	private void compact1(HosBloomFilter hosBloomFilter) throws IOException {
		File tmp = new File("tmp");
		if (!tmp.exists()) {
			boolean success = tmp.mkdirs();
			if (!success)
				throw new IOException("Could not create log directory "
						+ tmp.getAbsolutePath());
		}
		File index = new File(tmp, HosMetaData.INDEXFILE);
		HosWriter writer = Hos.createNew(index, CompressionType.SNAPPY,
				BLOCKSIZE);
		for (HosReader.Entry entry : reader) {
			byte[] key = entry.getKey();
			byte[] value = entry.getValue();
			if (!isChanged(key, value, hosBloomFilter)) {
				writer.put(key, value);
			}
		}
		// flush in memory data to external memory.
		this.putAll(memMap, writer);
		reader.close();
		writer.flush();
		writer.writeHash();
		writer.close();
		recover(tmp);
		saveDeletedIds();
	}


	private void recover(File tmp) throws IOException {
		// delete original file
		FileUtil.deleteOriginalFile(dir, HosMetaData.INDEXFILE,
				HosMetaData.DATAFILE);
		// temp file convert to dir.
		FileUtil.copyFile(new File(tmp,HosMetaData.INDEXFILE), 
				new File(dir, HosMetaData.INDEXFILE));
		FileUtil.copyFile(new File(tmp,HosMetaData.DATAFILE), 
				new File(dir, HosMetaData.DATAFILE));
		FileUtil.deleteFile(tmp);
	}

	private void saveDeletedIds() throws IOException {
		byte[] bigArray = ByteUtil.concatenateByteArrays(deletedIds);
		FileUtil.appendBytesToFile(new File(dir, HosMetaData.IDSFILE), bigArray);
	}
	
}
