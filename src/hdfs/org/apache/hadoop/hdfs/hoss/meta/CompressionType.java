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
import org.apache.hadoop.hdfs.hoss.meta.BlockPositionedInputStream;
import org.apache.hadoop.hdfs.hoss.meta.BlockRandomInput;
import org.apache.hadoop.hdfs.hoss.meta.ReadOnlyMemMap;
import org.apache.hadoop.hdfs.hoss.meta.SnappyOutputStream;
import org.apache.hadoop.hdfs.hoss.meta.SnappyRandomReader;
import org.apache.hadoop.hdfs.hoss.meta.SnappyReader;
import org.apache.hadoop.hdfs.hoss.meta.SnappyWriter;
import org.apache.hadoop.hdfs.hoss.meta.UncompressedBlockOutput;
import org.apache.hadoop.hdfs.hoss.meta.UncompressedBlockPositionedInputStream;
import org.apache.hadoop.hdfs.hoss.meta.UncompressedBlockRandomInput;

public enum CompressionType {
  NONE {
    @Override
    BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start) {
      return new UncompressedBlockPositionedInputStream(inputStream, start);
    }

    @Override
    BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize) {
      return new UncompressedBlockRandomInput(data);
    }

    @Override
    BlockOutput createBlockOutput(FileDescriptor fd, OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException {
      return new UncompressedBlockOutput(outputStream, fd);
    }

  },

  SNAPPY {
    @Override
    BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start) {
      return new SnappyReader(inputStream, maxBlockSize, start);
    }

    @Override
    BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize) {
      return new SnappyRandomReader(new UncompressedBlockRandomInput(data), maxBlockSize);
    }

    @Override
    BlockOutput createBlockOutput(FileDescriptor fd, OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException {
      return new SnappyWriter(new SnappyOutputStream(maxBlockSize, outputStream, fd), maxEntriesPerBlock);
    }
  },;

  abstract BlockOutput createBlockOutput(FileDescriptor fd, OutputStream outputStream, int maxBlockSize, int maxEntriesPerBlock) throws IOException;

  abstract BlockPositionedInputStream createBlockInput(InputStream inputStream, int maxBlockSize, long start);

  abstract BlockRandomInput createRandomAccessData(ReadOnlyMemMap data, int maxBlockSize);
}
