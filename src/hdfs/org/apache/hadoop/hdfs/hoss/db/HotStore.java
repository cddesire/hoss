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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.hoss.db.FileBlockStore.WriteBuffer;

public class HotStore {
	private static final Log LOG = LogFactory.getLog(HotStore.class);

	private FileBlockStore fbs = null;

	private final static long LIFESPAN = 12 * 60 * 60 * 1000;
	// the weight of object size
	private final static float ALPHA = 0.01f;
	// the weight of object access time
	private final static float BETA = 0.01f;

	private final static int HOTLENGTH = 32;

	public HotStore() {
		File hotFile = new File(new File(HosMetaData.HOSSDIR),
				HosMetaData.HOTFILE);
		fbs = new FileBlockStore(hotFile, HOTLENGTH, true);
		fbs.enableMmap();
		fbs.open();
	}

	public boolean put(long objId, long createTime, long lastTime, long size) {
		final WriteBuffer wbuf = fbs.set((int) objId);
		final ByteBuffer buf = wbuf.buf();
		// create time
		buf.putLong(createTime);
		// last access time
		buf.putLong(lastTime);
		long sizeMB = size < 0 ? -1 :convertMB(size); 
		// object size(unit:MB)
		buf.putLong(sizeMB);
		buf.flip();
		boolean ret = wbuf.save();
		return ret;
	}

	private long convertMB(long size) {
		return size / (1024 * 1024) + 1;
	}
	
	public long getObjectSize(long objId) {
		final ByteBuffer buf = fbs.get((int) objId);
		if (buf == null) {
			LOG.error("Error trying read object " + objId);
		}
		buf.getLong();
		buf.getLong();
		long size = buf.getLong();
		return size;
	}

	/**
	 * @param objId
	 * @return
	 */
	public float hot(long objId) {
		final ByteBuffer buf = fbs.get((int) objId);
		if (buf == null) {
			LOG.error("Error trying read object " + objId);
		}
		long current = System.currentTimeMillis();
		long createTime = buf.getLong();
		long lastTime = buf.getLong();
		long size = buf.getLong();

		put(objId, createTime, current, size);
		float hotness = ALPHA * sizeHot(size) + BETA
				* timeHot(current, createTime, lastTime);
		return hotness;
	}

	/**
	 * 
	 * @param objId
	 * @return
	 */
	public float firstHot(long objId, long size) {
		final ByteBuffer buf = fbs.get((int) objId);
		if (buf == null) {
			LOG.error("Error trying read object " + objId);
		}
		long current = System.currentTimeMillis();
		long createTime = buf.getLong();
		long lastTime = buf.getLong();
		buf.getLong();
		long sizeMB = convertMB(size);
		// update the last access time
		put(objId, createTime, current, sizeMB);
		float hotness = ALPHA * sizeHot(size) + BETA
				* timeHot(current, createTime, lastTime);
		return hotness;
	}

	private float sizeHot(long size) {
		float sizeHot = 0.0f;
		float eps = 1.0001f;
		sizeHot = (float) Math.max(0,
				5 - Math.floor(Math.log(size + eps) / Math.log(2)));
		sizeHot = (float) Math.pow(2, sizeHot);
		return sizeHot;
	}

	private float timeHot(long current, long createTime, long lastTime) {
		float timeHot = 0.0f;
		double valLast = LIFESPAN / (current - lastTime + 1);
		double valCreate = LIFESPAN / (current - createTime + 1);
		timeHot = (float) (Math.log(valLast * valCreate) / Math.log(2));
		return timeHot;
	}

	public void sync() {
		fbs.sync();
	}

	public void close() {
		fbs.sync();
		fbs.close();
	}

	public static void main(String[] args) throws InterruptedException {
		HotStore hs = new HotStore();
		long start = System.currentTimeMillis();
		Random random = new Random();
		for (int i = 0; i < 1000; i++) {
			long current = System.currentTimeMillis();
			long size = random.nextInt(1024 * 1024) * 1024;
			hs.put(i, current, current, size);
		}
		System.out.println("write total time: "
				+ (System.currentTimeMillis() - start) / 1000);
		start = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			TimeUnit.SECONDS.sleep(random.nextInt(10));
			int objId = random.nextInt(10);
			float hotness = hs.hot(objId);
			System.out.println(hotness);

		}
		System.out.println("read total time: "
				+ (System.currentTimeMillis() - start) / 1000);

		hs.close();

	}

}
