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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hdfs.hoss.meta.CompressionType;
import org.apache.hadoop.hdfs.hoss.meta.HashType;
import org.apache.hadoop.hdfs.hoss.meta.Hos;
import org.apache.hadoop.hdfs.hoss.meta.HosWriter;
import org.apache.hadoop.hdfs.hoss.meta.IndexHash;
import org.apache.hadoop.hdfs.hoss.meta.LogWriter;
import org.apache.hadoop.hdfs.hoss.meta.SingleThreadedHosWriter;

final class SingleThreadedHosWriter implements HosWriter {
  private final LogWriter logWriter;
  private final File logFile;
  private final File indexFile;
  private double sparsity;
  private HashType hashType;
  private boolean fsync;

  private SingleThreadedHosWriter(File indexFile, LogWriter logWriter) {
    this.logFile = logWriter.getFile();
    this.indexFile = indexFile;
    this.logWriter = logWriter;
  }

  static SingleThreadedHosWriter createNew(File file) throws IOException {
    return createNew(file, CompressionType.NONE, 0);
  }

  static SingleThreadedHosWriter createNew(File file, CompressionType compressionType, int blockSize) throws IOException {
    File indexFile = Hos.getIndexFile(file);
    if (indexFile.exists()) {
      indexFile.delete();
    }
    File logFile = Hos.getLogFile(file);
    if (logFile.exists()) {
      logFile.delete();
    }
    LogWriter logWriter = LogWriter.createNew(logFile, compressionType, blockSize);
    return new SingleThreadedHosWriter(indexFile, logWriter);
  }

  static SingleThreadedHosWriter append(File file) throws IOException {
    File logFile = Hos.getLogFile(file);
    if (!logFile.exists()) {
      throw new FileNotFoundException("File not found: " + logFile);
    }
    LogWriter writer = LogWriter.openExisting(logFile);

    File indexFile = Hos.getIndexFile(file);
    return new SingleThreadedHosWriter(indexFile, writer);
  }

  static SingleThreadedHosWriter appendOrCreate(File file, CompressionType type, int compressionBlockSize) throws IOException {
    File indexFile = Hos.getIndexFile(file);
    File logFile = Hos.getLogFile(file);

    LogWriter logWriter;
    if (logFile.exists()) {
      logWriter = LogWriter.openExisting(logFile);
    } else {
      logWriter = LogWriter.createNew(logFile, type, compressionBlockSize);
    }
    return new SingleThreadedHosWriter(indexFile, logWriter);
  }

  @Override
  public void close() throws IOException {
    logWriter.close(fsync);
  }

  @Override
  public void writeHash() throws IOException {
    flush();

    File parentFile = indexFile.getCanonicalFile().getParentFile();
    File newFile = new File(parentFile, indexFile.getName() + "-tmp" + System.currentTimeMillis());
    try {
      IndexHash.createNew(newFile, logFile, hashType, sparsity, fsync);
      boolean successful = newFile.renameTo(indexFile);
      if (!successful) {
        throw new IOException("Could not rename " + newFile + " to " + indexFile);
      }
    } finally {
      newFile.delete();
    }
  }

  @Override
  public void writeHash(HashType hashType) throws IOException {
    setHashType(hashType);
    writeHash();
  }

  public void setFsync(boolean fsync) {
    this.fsync = fsync;
  }

  @Override
  public void setHashType(HashType hashType) {
    this.hashType = hashType;
  }

  @Override
  public void setHashSparsity(double sparsity) {
    this.sparsity = sparsity;
  }

  @Override
  public void put(String key, String value) throws IOException {
    logWriter.put(key, value);
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    logWriter.put(key, value);
  }

  @Override
  public void put(byte[] key, InputStream valueStream, long valueLen) throws IOException {
    logWriter.put(key, valueStream, valueLen);
  }

  @Override
  public void delete(String key) throws IOException {
    logWriter.delete(key);
  }

  @Override
  public void delete(byte[] key) throws IOException {
    logWriter.delete(key);
  }

  @Override
  public void flush() throws IOException {
    logWriter.flush(fsync);
  }
}
