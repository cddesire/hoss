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
package org.apache.hadoop.hdfs.hoss.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hdfs.hoss.db.PathPosition;
import org.apache.hadoop.io.WritableComparable;


public class PathPosition implements WritableComparable<PathPosition>{
	
	private String path;
	private long offset;
	
	public PathPosition() {
		path = "";
		offset = -1;
	}
	
	public PathPosition(String path, long offset) {
		super();
		this.path = path;
		this.offset = offset;
	}
	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}
	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}
	/**
	 * @return the offset
	 */
	public long getOffset() {
		return offset;
	}
	
	/**
	 * @param offset the offset to set
	 */
	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.path = in.readUTF();
		this.offset = in.readLong();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(path);
		out.writeLong(offset);
	}
	
	
	@Override
	public int hashCode() {
		return (int) (path.hashCode() + offset % 163);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PathPosition) {
			PathPosition pp = (PathPosition) obj;
			return this.path.equals(pp.getPath())
					&& new Long(offset).equals(pp.getOffset());
		}
		return false;
	}

	@Override
	public String toString() {
		return path + ":" + offset;
	}

	@Override
	public int compareTo(PathPosition pp) {
		int cmp = path.compareTo(pp.getPath());
		if (cmp != 0) {
			return cmp;
		}
		return new Long(offset).compareTo(pp.getOffset());

	}

}
