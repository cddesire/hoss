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

package org.apache.hadoop.hdfs.hoss.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * String Serializer (ByteBuffer)
 * 
 */
public class StringSerializer {
	private static final Charset cs = Charset.forName("UTF-8");

	/**
	 * 
	 * @param out
	 * 
	 * @param str
	 */
	public static final void fromStringToBuffer(final ByteBuffer out,
			final String str) {
		if (str == null) {
			out.putInt(Integer.MIN_VALUE);
			return;
		}
		final byte[] bytes = str.getBytes(cs);
		final int len = bytes.length;
		out.putInt(len);
		out.put(bytes, 0, len);
	}
	
	/**
	 * 
	 * @param out
	 * @param str
	 * @param length
	 */
	public static final void fromStringToBuffer(final ByteBuffer out,
			final String str, final int length) {
		if (str == null) {
			out.putInt(length);
			return;
		}
		final byte[] bytes = str.getBytes(cs);
		out.put(bytes, 0, length);
	}

	/**
	 * 
	 * @param in
	 * @return
	 */
	public static final String fromBufferToString(final ByteBuffer in) {
		final int len = in.getInt();
		if (len == Integer.MIN_VALUE) {
			return null;
		}
		final byte[] bytes = new byte[len];
		in.get(bytes, 0, len);
		return new String(bytes, 0, len, cs);
	}
	
	
	/**
	 * 
	 * @param in
	 * @param length
	 * @return
	 */
	public static final String fromBufferToString(final ByteBuffer in, final int length) {
		final byte[] bytes = new byte[length];
		in.get(bytes, 0, length);
		return new String(bytes, 0, length, cs);
	}
	

}
