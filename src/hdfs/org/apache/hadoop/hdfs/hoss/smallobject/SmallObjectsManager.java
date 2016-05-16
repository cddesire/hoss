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
package org.apache.hadoop.hdfs.hoss.smallobject;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.hoss.db.HosMetaData;
import org.apache.hadoop.hdfs.hoss.db.PathPosition;
import org.apache.hadoop.hdfs.hoss.util.HDFSUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

public class SmallObjectsManager {
	
	private static final Log LOG = LogFactory.getLog(SmallObjectsManager.class);

	protected static FileSystem fs = HDFSUtil.getFileSystem();

	private HosMetaData metaDataDb = null;

	private static final String ACTIVEOBJECT = "activeobject";

	public static final String STABLEOBJECT = "0000000000";

	public SmallObjectsManager(HosMetaData hmd) {
		metaDataDb = hmd;
	}

	/**
	 * combine small object
	 */
	public boolean combine() {
		Map<Long, Integer> smallObjects = metaDataDb.smallObjects();
		boolean success = false;
		boolean isCombined = true;
		Configuration conf = new Configuration();
		if (smallObjects.size() != 0) {
			SequenceFile.Writer writer = null;
			try {
				writer = SequenceFile.createWriter(fs, conf, new Path(
						ACTIVEOBJECT), KeyWritable.class, ValueWritable.class);
				success = batchWrite(writer, smallObjects, conf);
				LOG.info("Batch write objects successfully");
			} catch (IOException e) {
				LOG.error("Combine small object initilize SequenceFile Writer error: "
						+ e);
			} finally {
				IOUtils.closeStream(writer);
			}

			if (success) {
				updateMetadata(conf);
				metaDataDb.setNameId("super object", 0);
				try {
					fs.rename(new Path(ACTIVEOBJECT), new Path(STABLEOBJECT));
				} catch (IOException e) {
					LOG.error("Rename super object container error: " + e);
				}
				LOG.info("Update small objects metadata successfully");
			}
			//metaDataDb.listPathPosition();
		} else {
			isCombined = false;
			LOG.info("The number of small objects is too little");
		}
		return isCombined;
	}

	/**
	 * batch write small object to sequence file
	 * 
	 * @param writer
	 * @param smallObjects
	 * @return
	 */
	private boolean batchWrite(SequenceFile.Writer writer,
			Map<Long, Integer> smallObjects, Configuration conf) {
		boolean suc = false;
		boolean suc1 = updateCombinedObjects(writer, smallObjects, conf);
		boolean suc2 = writeNewSmallObjects(writer, smallObjects);
		suc = suc1 & suc2;
		return suc;
	}

	/**
	 * write new small objects
	 * 
	 * @param writer
	 * @param smallObjects
	 * @return
	 */
	private boolean writeNewSmallObjects(SequenceFile.Writer writer,
			Map<Long, Integer> smallObjects) {
		boolean success = true;
		for (Entry<Long, Integer> entry : smallObjects.entrySet()) {
			long objId = entry.getKey();
			int size = entry.getValue();
			byte[] value = readFully(objId, size);
			KeyWritable kw = new KeyWritable(objId, size);
			ValueWritable vw = new ValueWritable(value, "object " + objId);
			try {
				writer.append(kw, vw);
			} catch (IOException e) {
				success = false;
				LOG.error("Write new small objects  " + objId + " error. " + e);
			}
		}
		return success;
	}

	private boolean updateCombinedObjects(SequenceFile.Writer writer,
			Map<Long, Integer> smallObjects, Configuration conf) {
		boolean success = true;
		SequenceFile.Reader reader = null;

		Path stable = new Path(STABLEOBJECT);
		boolean exist = exists(stable);
		// stable object is not exist
		if (!exist) {
			return true;
		}

		try {
			reader = new SequenceFile.Reader(fs, stable, conf);
			KeyWritable kw = (KeyWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			ValueWritable vw = (ValueWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			while (reader.next(kw, vw)) {
				long objId = kw.getObjId();
				// if combined object is deleted, object id is reused
				// small object / object / unused
				if (!(smallObjects.containsKey(objId) || metaDataDb
						.exist(objId))) {
					writer.append(kw, vw);
				}
			}
		} catch (IOException e) {
			success = false;
			LOG.error("combine small object initilize SequenceFile Reader error: "
					+ e);
		} finally {
			IOUtils.closeStream(reader);
		}
		if (success) {
			deleteOriginalObject(STABLEOBJECT);
		}
		return success;
	}

	private boolean exists(Path path) {
		boolean exist = false;
		try {
			exist = fs.exists(path);
		} catch (IOException e1) {
			LOG.error(path.toString() + "is not exist error " + e1);
		}
		return exist;
	}

	private synchronized byte[] readFully(long objId, int size) {
		FSDataInputStream in = null;
		byte[] buf = new byte[size];
		PathPosition pp = metaDataDb.getPathPosition(objId);
		try {
			in = fs.open(new Path(pp.getPath()));
			IOUtils.readFully(in, buf, 0, size);
		} catch (IOException e) {
			LOG.error("combine small object initilize FSDataInputStream error: "
					+ e);
		} finally {
			IOUtils.closeStream(in);
		}
		return buf;
	}

	/**
	 * update the object metadata after combining
	 */
	private void updateMetadata(Configuration conf) {
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, new Path(ACTIVEOBJECT), conf);
			KeyWritable kw = (KeyWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			ValueWritable vw = (ValueWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			long offset = reader.getPosition();
			while (reader.next(kw, vw)) {
				long objId = kw.getObjId();
				PathPosition pp = metaDataDb.getPathPosition(objId);
				String originalPath = pp.getPath();
				// uncombined file
				if (pp.getOffset() == 0) {
					deleteOriginalObject(originalPath);
				}
				// pathID is 0 because it is a super large object container
				metaDataDb.updatePathPos(objId, 0L, offset);
				//LOG.info("updatePathPos objID " + objId + " offset " + offset);
				offset = reader.getPosition();
			}
		} catch (IOException e) {
			LOG.error("combine small object initilize SequenceFile Reader error: "
					+ e);
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	private void deleteOriginalObject(String objPath) {
		try {
			fs.delete(new Path(objPath), false);
		} catch (IOException e) {
			LOG.error("delete original object error " + e);
		}
	}
	/**
	 * byte array for small object 
	 *
	 */
	public byte[] getSmallObject(long objId, long offset) {
		SequenceFile.Reader reader = null;
		byte[] bytes = null;
		try {
			Configuration conf = new Configuration();
			reader = new SequenceFile.Reader(fs, new Path(STABLEOBJECT), conf);
			KeyWritable kw = (KeyWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			ValueWritable vw = (ValueWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			reader.seek(offset);
			reader.next(kw, vw);
			if (kw.getObjId() == objId) {
				bytes = vw.getValue();
			} else {
				LOG.error("read small object mismatch ");
			}
		} catch (IOException e) {
			LOG.error("combine small object initilize SequenceFile Reader error: "
					+ e);
		} finally {
			IOUtils.closeStream(reader);
		}
		return bytes;
	}

}
