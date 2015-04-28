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

package org.apache.hadoop.hdfs.hoss.meta.extra;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hdfs.hoss.meta.Hos;
import org.apache.hadoop.hdfs.hoss.meta.HosReader;
import org.apache.hadoop.hdfs.hoss.meta.extra.AbstractDelegatingHosReader;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A thread-safe hoss Reader.
 */
public class ThreadLocalHosReader extends AbstractDelegatingHosReader {
	
	private final Collection<HosReader> readers = Lists.newArrayList();
	
	private volatile ThreadLocal<HosReader> threadLocalReader;

	public ThreadLocalHosReader(File indexFile) throws IOException {
		this(Hos.open(indexFile));
	}

	public ThreadLocalHosReader(final HosReader reader) {
		checkNotNull(reader, "reader may not be null");

		this.readers.add(reader);
		this.threadLocalReader = new ThreadLocal<HosReader>() {
			@Override
			protected HosReader initialValue() {
				HosReader r = reader.duplicate();
				synchronized (readers) {
					readers.add(r);
				}
				return r;
			}
		};
	}

	@Override
	public void close() throws IOException {
		this.threadLocalReader = null;
		synchronized (readers) {
			for (HosReader reader : readers) {
				reader.close();
			}
			readers.clear();
		}
	}

	@Override
	public HosReader duplicate() {
		checkState(threadLocalReader != null, "reader is closed");
		return this;
	}

	@Override
	protected HosReader getDelegateReader() {
		checkState(threadLocalReader != null, "reader is closed");
		return threadLocalReader.get();
	}

}
