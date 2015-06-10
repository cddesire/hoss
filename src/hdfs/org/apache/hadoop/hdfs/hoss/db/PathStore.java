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
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.hoss.db.FileBlockStore;
import org.apache.hadoop.hdfs.hoss.db.HosMetaData;
import org.apache.hadoop.hdfs.hoss.db.PathPosition;
import org.apache.hadoop.hdfs.hoss.db.FileBlockStore.WriteBuffer;
import org.apache.hadoop.hdfs.hoss.util.StringSerializer;



public class PathStore {
	
	private static final Log LOG = LogFactory.getLog(PathStore.class);
	
	private FileBlockStore fbs;
	
	private final static int PATHLENGTH = 32;
	
	private final static int PATHWIDTH = 10;
	
	//private final static int BLOCKSIZE = 4 * 1024; // 4KB
	
	//private final static int PATHCNT = BLOCKSIZE / PATHLENGTH;
	
	//private volatile int numBlock;
	
	
	public PathStore() {
		File pathFile = new File(new File(HosMetaData.HOSSDIR), HosMetaData.PATHFILE);
		fbs = new FileBlockStore(pathFile, PATHLENGTH, true);
		fbs.enableMmap();
		fbs.open();
	}
	
	private String getFixedPath(long id) {
		return fixedLengthString(id, PATHWIDTH);
	}
	
	public static String fixedLengthString(long num, int width) {
	    return String.format("%0" + width + "d", num);
	}

	/**
	 * 
	 * @param pathId
	 * @param objId:the index-th block in fileblockstore
	 * @param offset
	 * @return
	 */
	public PathPosition put(long objId, long pathId, long offset) {
		String path = getFixedPath(pathId);
		//set the block index in the fileblockstore
		final WriteBuffer wbuf = fbs.set((int)objId);
		final ByteBuffer buf = wbuf.buf();
		StringSerializer.fromStringToBuffer(buf, path, PATHWIDTH);
		buf.putLong(offset);
		buf.flip();
		wbuf.save();
		PathPosition pp = new PathPosition(path, offset);
		return pp;
	}
	
	/**
	 * put path and offset to file
	 * @param objId
	 * @param offset
	 * @return
	 */
	private PathPosition put(long objId, long offset) {
		return put(objId, objId, offset);//objID+pathID+offset
	}
	
	public PathPosition put(long objId) {
		return put(objId, 0);
	}
	
	/**
	 * get path and offset given object id 
	 * @param objId
	 * @return
	 */
	public PathPosition get(long objId) {
		final ByteBuffer buf = fbs.get((int)objId);
		if (buf == null) {
			LOG.error("Error trying read object " + objId);
		}
		String path = StringSerializer.fromBufferToString(buf, PATHWIDTH);
		long offset = buf.getLong();
		PathPosition pp = new PathPosition(path, offset);
		return pp;
	}
	
	public void sync() {
		fbs.sync();
	}
	
	public void close() {
		fbs.sync();
		fbs.close();
	}

}
