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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.hoss.bloomfilter.BloomFilter;
import org.apache.hadoop.hdfs.hoss.bloomfilter.HosBloomFilter;
import org.apache.hadoop.hdfs.hoss.db.HosMetaData;


public class HosBloomFilter {
	
	private static final Log LOG = LogFactory.getLog(HosBloomFilter.class);
	
	private final File onDiskFile = new File(new File(HosMetaData.HOSSDIR), 
			HosMetaData.BLOOMFILTER);
	
	private final int expectedItems = 40000000;
	
	private final double desiredFalsePositiveRate = 0.01;
	
	private BloomFilter bloomFilter = null;

	public HosBloomFilter(int expectedItems, double desiredFalsePositiveRate)
			throws IOException {
		bloomFilter = new BloomFilter.NewBuilder(onDiskFile, expectedItems,
				desiredFalsePositiveRate).force(true).build();
	}

	public HosBloomFilter() {
		if(!onDiskFile.exists()){
			try {
				bloomFilter = new BloomFilter.NewBuilder(onDiskFile, expectedItems,
					desiredFalsePositiveRate).force(true).build();
			} catch (IOException e) {
				LOG.error("hos bloom filter create faild duo to " + e);
			}
		}
		try {
			bloomFilter = new BloomFilter.OpenBuilder(onDiskFile).build();
		} catch (IOException e) {
			LOG.error("hos bloom filter create from disk faild duo to " + e);
		}
		
	}
	
	public void add(String data) {
		if(bloomFilter.contains(data.getBytes(Charset.forName("UTF-8")))){
			System.out.println("******* " + data);
		}
		bloomFilter.add(data.getBytes(Charset.forName("UTF-8")));
	}
	
	public void remove(String data) {
		bloomFilter.remove(data.getBytes(Charset.forName("UTF-8")));
	}
	
	public boolean contain(String data) {
		return bloomFilter.contains(data.getBytes(Charset.forName("UTF-8")));
	}
	
	public void close() throws IOException {
		bloomFilter.close();
		bloomFilter = null;
	}

	public static void main(String[] args) throws IOException {
		HosBloomFilter hosBloomFilter = new HosBloomFilter();
		
		for(int i = 0; i < 1000000; i++){
			hosBloomFilter.add("hehe" + i);
		}
		
		
		hosBloomFilter.close();
		
		hosBloomFilter = new HosBloomFilter();
		System.out.println(hosBloomFilter.contain("hehe2"));
		System.out.println(hosBloomFilter.contain("hehe1"));
		System.out.println(hosBloomFilter.contain("hehe3"));
		
	}

}
