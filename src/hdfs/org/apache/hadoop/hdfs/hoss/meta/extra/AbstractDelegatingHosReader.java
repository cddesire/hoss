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


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hdfs.hoss.meta.extra.AbstractDelegatingHosReader;
import org.apache.hadoop.hdfs.hoss.meta.HosReader;
import org.apache.hadoop.hdfs.hoss.meta.IndexHeader;
import org.apache.hadoop.hdfs.hoss.meta.LogHeader;

/**
 * A superclass for Sparkey readers that delegate to another
 * {@link HosReader}.
 * 
 * Subclasses must override the
 * {@link AbstractDelegatingHosReader#getDelegateReader()} method.
 */
public abstract class AbstractDelegatingHosReader implements HosReader {

	protected abstract HosReader getDelegateReader();

	@Override
	public String getAsString(String key) throws IOException {
		return getDelegateReader().getAsString(key);
	}

	@Override
	public byte[] getAsByteArray(byte[] key) throws IOException {
		return getDelegateReader().getAsByteArray(key);
	}

	@Override
	public Entry getAsEntry(byte[] key) throws IOException {
		return getDelegateReader().getAsEntry(key);
	}

	@Override
	public void close() throws IOException {
		getDelegateReader().close();
	}

	@Override
	public IndexHeader getIndexHeader() {
		return getDelegateReader().getIndexHeader();
	}

	@Override
	public LogHeader getLogHeader() {
		return getDelegateReader().getLogHeader();
	}

	@Override
	public HosReader duplicate() {
		return getDelegateReader().duplicate();
	}

	@Override
	public Iterator<Entry> iterator() {
		return getDelegateReader().iterator();
	}

}
