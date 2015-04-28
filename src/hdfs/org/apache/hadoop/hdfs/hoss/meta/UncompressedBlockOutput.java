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

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.hoss.meta.BlockOutput;
import org.apache.hadoop.hdfs.hoss.meta.Util;

final class UncompressedBlockOutput implements BlockOutput {
  private final byte[] buf = new byte[1024*1024];
  private final OutputStream outputStream;
  private final FileDescriptor fileDescriptor;

  UncompressedBlockOutput(OutputStream outputStream, FileDescriptor fileDescriptor) {
    this.outputStream = outputStream;
    this.fileDescriptor = fileDescriptor;
  }

  @Override
  public void put(byte[] key, int keyLen, byte[] value, int valueLen) throws IOException {
    Util.writeUnsignedVLQ(keyLen + 1, outputStream);
    Util.writeUnsignedVLQ(valueLen, outputStream);
    outputStream.write(key, 0, keyLen);
    outputStream.write(value, 0, valueLen);
  }

  @Override
  public void put(byte[] key, int keyLen, InputStream value, long valueLen) throws IOException {
    Util.writeUnsignedVLQ(keyLen + 1, outputStream);
    Util.writeUnsignedVLQ(valueLen, outputStream);
    outputStream.write(key, 0, keyLen);
    Util.copy(valueLen, value, outputStream, buf);
  }

  @Override
  public void delete(byte[] key, int keyLen) throws IOException {
    outputStream.write(0);
    Util.writeUnsignedVLQ(keyLen, outputStream);
    outputStream.write(key, 0, keyLen);
  }

  @Override
  public void flush(boolean fsync) throws IOException {
    outputStream.flush();
    if (fsync) {
      fileDescriptor.sync();
    }
  }

  @Override
  public void close(boolean fsync) throws IOException {
    flush(fsync);
    outputStream.close();
  }

  @Override
  public int getMaxEntriesPerBlock() {
    return 1;
  }
}
