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
package org.apache.hadoop.hdfs.hoss.meta;

import com.google.common.base.Charsets;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hdfs.hoss.meta.Hos;
import org.apache.hadoop.hdfs.hoss.meta.HosLogIterator;
import org.apache.hadoop.hdfs.hoss.meta.HosReader;
import org.apache.hadoop.hdfs.hoss.meta.IndexHash;
import org.apache.hadoop.hdfs.hoss.meta.IndexHeader;
import org.apache.hadoop.hdfs.hoss.meta.LogHeader;
import org.apache.hadoop.hdfs.hoss.meta.SingleThreadedHosReader;

final class SingleThreadedHosReader implements HosReader {
	private final IndexHash index;
	private final File indexFile;
	private final File logFile;
	private final IndexHeader header;
	private final LogHeader logHeader;

	private SingleThreadedHosReader(File indexFile, File logFile)
			throws IOException {
		this(indexFile, logFile, IndexHash.open(indexFile, logFile));
	}

	private SingleThreadedHosReader(File indexFile, File logFile,
			IndexHash index) {
		this.index = index;
		this.indexFile = indexFile;
		this.logFile = logFile;
		header = index.header;
		logHeader = index.logHeader;
	}

	@Override
	public SingleThreadedHosReader duplicate() {
		return new SingleThreadedHosReader(indexFile, logFile, index
				.duplicate());
	}

	static SingleThreadedHosReader open(File file) throws IOException {
		return new SingleThreadedHosReader(Hos.getIndexFile(file),
				Hos.getLogFile(file));
	}

	@Override
	public void close() throws IOException {
		index.close();
	}

	@Override
	public String getAsString(String key) throws IOException {
		byte[] keyBytes = key.getBytes(Charsets.UTF_8);
		Entry res = getAsEntry(keyBytes);
		if (res == null) {
			return null;
		}
		return new String(res.getValue(), Charsets.UTF_8);
	}

	@Override
	public byte[] getAsByteArray(byte[] key) throws IOException {
		Entry entry = getAsEntry(key);
		if (entry == null) {
			return null;
		}
		return entry.getValue();
	}

	@Override
	public HosReader.Entry getAsEntry(byte[] key) throws IOException {
		return index.get(key.length, key);
	}

	/**
	 * @return a new iterator that can be safely used from a single thread. Note
	 *         that antries will be reused and modified, so any data you want
	 *         from it must be consumed before continuing iteration. You should
	 *         not pass this entry on in any way.
	 */
	@Override
	public Iterator<HosReader.Entry> iterator() {
		HosLogIterator logIterator;
		final IndexHash indexHash;
		try {
			logIterator = new HosLogIterator(logFile, -1, index.header
					.getDataEnd());
			indexHash = index.duplicate();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		final Iterator<HosReader.Entry> iterator = logIterator.iterator();

		return new Iterator<HosReader.Entry>() {
			private HosReader.Entry entry;
			private boolean ready;

			public boolean hasNext() {
				if (ready) {
					return true;
				}
				while (iterator.hasNext()) {
					// Safe cast, since the iterator is guaranteed to be a
					// SparkeyLogIterator
					HosLogIterator.Entry next = (HosLogIterator.Entry) iterator
							.next();

					if (next.getType() == HosReader.Type.PUT) {
						int keyLen = next.getKeyLength();
						try {
							if (isValid(keyLen, next.getKeyBuf(), next
									.getPosition(), next.getEntryIndex(),
									indexHash)) {
								entry = next;
								ready = true;
								return true;
							}
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				}
				return false;
			}

			public Entry next() {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
				ready = false;
				Entry localEntry = entry;
				entry = null;
				return localEntry;
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	private static boolean isValid(int keyLen, byte[] keyBuf, long position,
			int entryIndex, IndexHash indexHash) throws IOException {
		return indexHash.isAt(keyLen, keyBuf, position, entryIndex);
	}

	@Override
	public IndexHeader getIndexHeader() {
		return header;
	}

	@Override
	public LogHeader getLogHeader() {
		return logHeader;
	}

}
