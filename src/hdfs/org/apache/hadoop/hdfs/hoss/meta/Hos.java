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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.hoss.meta.CompressionType;
import org.apache.hadoop.hdfs.hoss.meta.HashType;
import org.apache.hadoop.hdfs.hoss.meta.HosReader;
import org.apache.hadoop.hdfs.hoss.meta.HosWriter;
import org.apache.hadoop.hdfs.hoss.meta.IndexHeader;
import org.apache.hadoop.hdfs.hoss.meta.LogHeader;
import org.apache.hadoop.hdfs.hoss.meta.SingleThreadedHosReader;
import org.apache.hadoop.hdfs.hoss.meta.SingleThreadedHosWriter;

public final class Hos {
	private Hos() {
	}

	/**
	 * Creates a new hoss writer without any compression.
	 * 
	 * This is not a thread-safe class, only use it from one thread.
	 * 
	 * @param file
	 *            File base to use, the actual file endings will be set to .spi
	 *            and .spl
	 * @return a new writer,
	 */
	public static HosWriter createNew(File file) throws IOException {
		return SingleThreadedHosWriter.createNew(file);
	}

	/**
	 * Creates a new hoss writer with the specified compression
	 * 
	 * This is not a thread-safe class, only use it from one thread.
	 * 
	 * @param file
	 *            File base to use, the actual file endings will be set to .spi
	 *            and .spl
	 * @param compressionType
	 * @param compressionBlockSize
	 *            The maximum compression block size in bytes
	 * @return a new writer,
	 */
	public static HosWriter createNew(File file,
			CompressionType compressionType, int compressionBlockSize)
			throws IOException {
		return SingleThreadedHosWriter.createNew(file, compressionType,
				compressionBlockSize);
	}

	/**
	 * Opens an existing file for append.
	 * 
	 * This is not a thread-safe class, only use it from one thread.
	 * 
	 * @param file
	 *            File base to use, the actual file endings will be set to .spi
	 *            and .spl
	 * @return a new writer,
	 */
	public static HosWriter append(File file) throws IOException {
		return SingleThreadedHosWriter.append(file);
	}

	/**
	 * Opens an existing file for append, or create a new one if it doesn't
	 * exist. The compression settings will only apply for new files.
	 * 
	 * This is not a thread-safe class, only use it from one thread.
	 * 
	 * @param file
	 *            File base to use, the actual file endings will be set to .spi
	 *            and .spl
	 * @param compressionType
	 * @param compressionBlockSize
	 *            The maximum compression block size in bytes
	 * @return a new writer,
	 */
	public static HosWriter appendOrCreate(File file,
			CompressionType compressionType, int compressionBlockSize)
			throws IOException {
		return SingleThreadedHosWriter.appendOrCreate(file,
				compressionType, compressionBlockSize);
	}

	/**
	 * Open a new hoss reader
	 * 
	 * This is not a thread-safe class, only use it from one thread.
	 * 
	 * @param file
	 *            File base to use, the actual file endings will be set to .spi
	 *            and .spl
	 * @return a new reader,
	 */
	public static HosReader open(File file) throws IOException {
		return SingleThreadedHosReader.open(file);
	}

	/**
	 * Write (or rewrite) the index file for a given hoss file.
	 * 
	 * @param file
	 *            File base to use, the actual file endings will be set to .spi
	 *            and .spl
	 */
	public static void writeHash(File file) throws IOException {
		HosWriter writer = append(file);
		writer.writeHash();
		writer.close();
	}

	/**
	 * Write (or rewrite) the index file for a given hoss file.
	 * 
	 * @param file
	 *            File base to use, the actual file endings will be set to .spi
	 *            and .spl
	 * @param hashType
	 *            choice of hash type, can be 32 or 64 bits.
	 */
	@SuppressWarnings("deprecation")
	public static void writeHash(File file, HashType hashType)
			throws IOException {
		HosWriter writer = append(file);
		writer.writeHash(hashType);
		writer.close();
	}

	/**
	 * Sets the file ending of the file to .spl (the log filename convention)
	 * 
	 * @param file
	 * @return a file object with .spl as file ending
	 */
	public static File getLogFile(File file) {
		return setEnding(file, ".spl");
	}

	/**
	 * Sets the file ending of the file to .spi (the index filename convention)
	 * 
	 * @param file
	 * @return a file object with .spi as file ending
	 */
	public static File getIndexFile(File file) {
		return setEnding(file, ".spi");
	}

	private static File setEnding(File file, String ending) {
		if (file.getName().endsWith(ending)) {
			return file;
		}
		return new File(file.getParentFile(), changeEnding(file.getName(),
				ending));
	}

	private static String changeEnding(String s, String ending) {
		int index = s.lastIndexOf(".");
		if (index == -1) {
			return s + ending;
		}
		return s.substring(0, index) + ending;
	}

	/**
	 * Extract the header information from the index file.
	 * 
	 * @param file
	 * @return an index header
	 */
	public static IndexHeader getIndexHeader(File file) throws IOException {
		return IndexHeader.read(file);
	}

	/**
	 * Extract the header information from the log file.
	 * 
	 * @param file
	 * @return an index header
	 */
	public static LogHeader getLogHeader(File file) throws IOException {
		return LogHeader.read(file);
	}
}
