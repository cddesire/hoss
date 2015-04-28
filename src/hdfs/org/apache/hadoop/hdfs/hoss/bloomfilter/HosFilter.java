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
package org.apache.hadoop.hdfs.hoss.bloomfilter;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.hdfs.hoss.bloomfilter.HosFilter;
import org.apache.hadoop.hdfs.hoss.db.FileStreamStore;
import org.apache.hadoop.hdfs.hoss.db.HosMetaData;
import org.apache.hadoop.hdfs.hoss.util.StringSerializer;



public class HosFilter {
	
	private FileStreamStore fss = null;
	
	private ConcurrentSkipListSet<String> filter = null;
	
	private static final String HOSFILTER = HosMetaData.HOSSDIR + "/hosfilter";
	
	private static final int BUFFERLEN = 4 * 1024;
	
	final ByteBuffer buf = ByteBuffer.allocate(BUFFERLEN);
	
	public HosFilter() {
		filter = new ConcurrentSkipListSet<String>();
		fss = new FileStreamStore(HOSFILTER, BUFFERLEN);
		fss.open();
		fss.setFlushOnWrite(false);
		fss.setSyncOnFlush(false);
		fss.setAlignBlocks(true);
		initFilter();
	}
	
	private void initFilter() {
		if (!fss.isEmpty()) {
			long newOffset = 0;
			while (!(newOffset < 0)) {
				buf.clear();
				newOffset = fss.read(newOffset, buf);
				String data = StringSerializer.fromBufferToString(buf);
				filter.add(data);
			}
		}
	}
	
	public void add(String item) {
		filter.add(item);
	}
	
	public boolean remove(String item) {
		return filter.remove(item);
	}
	
	public boolean contains(String item) {
		return filter.contains(item);
	}
	
	public void flush() {
		fss.clear();
		for(String item: filter) {
			buf.clear();
			StringSerializer.fromStringToBuffer(buf, item);
			buf.flip();
			fss.write(buf);
		}
	}
	
	public void close() {
		flush();
		fss.close();
		fss = null;
		filter = null;
	}
	
	public static void main(String[] args) {
		HosFilter hf = new HosFilter();
		for (int i = 0; i < 100000; i++){
			hf.add("hehe" + i);
		}
		
		hf.close();
		
		hf = new HosFilter();
		hf.remove("hehe100");
		hf.remove("hehe200");
		hf.remove("hehe300");
		for(int i = 0; i < 100000; i++) {
			String expceted = "hehe" + i;
			boolean exist = hf.contains(expceted);
			if(!exist){
				System.out.println(expceted);
			}
		}
	}

}
