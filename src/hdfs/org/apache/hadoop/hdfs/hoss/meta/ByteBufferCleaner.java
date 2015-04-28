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

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This code was taken from lucene. See NOTICE file for more details.
 */
class ByteBufferCleaner {

  /**
   * <code>true</code>, if this platform supports unmapping mmapped files.
   */
  public static final boolean UNMAP_SUPPORTED;
  static {
    boolean v;
    try {
      Class.forName("sun.misc.Cleaner");
      Class.forName("java.nio.DirectByteBuffer")
              .getMethod("cleaner");
      v = true;
    } catch (Exception e) {
      v = false;
    }
    UNMAP_SUPPORTED = v;
  }

  /**
   * Try to unmap the buffer, this method silently fails if no support
   * for that in the JVM. On Windows, this leads to the fact,
   * that mmapped files cannot be modified or deleted.
   */
  public static void cleanMapping(final MappedByteBuffer buffer) {
    if (!UNMAP_SUPPORTED) {
      return;
    }

    try {
      AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
        public Object run() throws Exception {
          final Method getCleanerMethod = buffer.getClass()
                  .getMethod("cleaner");
          getCleanerMethod.setAccessible(true);

          final Object cleaner = getCleanerMethod.invoke(buffer);
          if (cleaner != null) {
            cleaner.getClass().getMethod("clean").invoke(cleaner);
          }
          return null;
        }
      });
    } catch (PrivilegedActionException e) {
      final IOException ioe = new IOException("unable to unmap the mapped buffer");
      ioe.initCause(e.getCause());
      e.printStackTrace(System.err);
    }
  }
}
