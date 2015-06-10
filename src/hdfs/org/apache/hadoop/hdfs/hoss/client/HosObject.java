package org.apache.hadoop.hdfs.hoss.client;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.hoss.db.PathPosition;
import org.apache.hadoop.hdfs.hoss.smallobject.KeyWritable;
import org.apache.hadoop.hdfs.hoss.smallobject.SmallObjectsManager;
import org.apache.hadoop.hdfs.hoss.smallobject.ValueWritable;
import org.apache.hadoop.hdfs.hoss.util.HDFSUtil;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

public class HosObject {

	private static final Log LOG = LogFactory.getLog(HosObject.class);

	protected static FileSystem fs = HDFSUtil.getFileSystem();

	private FSDataInputStream in = null;

	private FSDataOutputStream out = null;

	private static final int BUFFERSIZE = 4 * 1024;

	private static final long BLOCKSIZE = 64 * 1024 * 1024L;

	private static ClientProtocol client = null;

	private String objName = null;

	static {
		client = HosClient.client();
	}

	public HosObject(final String objName) {
		this.objName = objName;
	}

	private PathPosition putObject() {
		return client.putObject(objName);
	}

	private long getObjectId() {
		return client.getObjectId(objName);
	}

	private PathPosition getPathPosition() {
		return client.getPathPosition(objName);
	}

	private long deleteObj() {
		return client.deleteObject(objName);
	}

	private boolean isExist() {
		return client.exist(objName);
	}

	private boolean initWriter(short replication, boolean overwrite) {
		if (isExist() && !overwrite) {
			LOG.warn("object  " + objName
					+ " already exists, and doesn't permit overwrite.");
			return false;
		}
		PathPosition pp = this.putObject();
		if (pp != null) {
			try {
				out = fs.create(new Path(pp.getPath()), overwrite, BUFFERSIZE,
						replication, BLOCKSIZE);
			} catch (IOException e) {
				LOG.error("Initalize FSDataOutputStream error: " + e);
			}
		}
		return true;
	}
	
		
	/**
	 * init the object input stream 
	 * @return
	 */
	private boolean initReader() {
		if (!isExist()) {
			LOG.warn("object  " + objName + " not exists");
			return false;
		}
		PathPosition pp = this.getPathPosition();
		Path path = new Path(pp.getPath());
		long offset = pp.getOffset();
		//LOG.info("initReader  id " + getObjectId() + " offset " + offset);
		//read combined small object
		if (offset > 0) {
			byte[] buf = getSmallObject(getObjectId(), offset);	
			try {
				if(buf != null){
				in = new FSDataInputStream(new BufferedFSInputStream(
						               new BytesInputStream(buf), buf.length)) ;
				} else {
					return false;
				}
			} catch (IOException e) {
				LOG.error("in combine initalize FSDataInputStream error: " + e);
			}
		} else {
			try {
				in = fs.open(path);
			} catch (IOException e) {
				LOG.error("in uncombined initalize FSDataInputStream error: " + e);
			}
		}
		return true;
	}

	/**
	 * get output stream
	 * 
	 * @param replication
	 * @param overwrite
	 * @return
	 */
	public FSDataOutputStream getWriter(short replication, boolean overwrite) {
		FSDataOutputStream writer = null;
		if (initWriter(replication, overwrite)) {
			writer = this.out;
		}
		return writer;
	}

	/**
	 * get output stream with no overwrite
	 * 
	 * @param replication
	 * @return
	 */
	public FSDataOutputStream getWriter(short replication) {
		return getWriter(replication, false);
	}

	public FSDataOutputStream getWriter() {
		return getWriter((short) 1, false);
	}

	/**
	 * get input stream
	 * 
	 * @return
	 */
	public FSDataInputStream getReader() {
		FSDataInputStream reader = null;
		if (initReader()) {
			reader = this.in;
		}
		return reader;
	}

	public void close() {
		if (in != null) {
			IOUtils.closeStream(in);
		}

		if (out != null) {
			IOUtils.closeStream(out);
		}
	}

	/**
	 * delete hos object
	 */
	public void deleteObject() {
		boolean flag = false;
		if (!isExist()) {
			LOG.warn("object  " + objName + " not exists");
			return;
		}
		PathPosition pp = this.getPathPosition();
		Path path = new Path(pp.getPath());
		long offset = pp.getOffset();
		try {
			if(offset == 0){
				flag = fs.delete(path, false);
			}
			this.deleteObj();
		} catch (IOException e) {
			LOG.error(e);
		}
		if (offset == 0 && !flag) {
			LOG.warn("delete object " + objName + "fail");
		}
	}
	
	public byte[] getSmallObject(long objId, long offset){
		SequenceFile.Reader reader = null;
		byte[] bytes = null;
		try {
			String superObj = SmallObjectsManager.STABLEOBJECT;
			reader = new SequenceFile.Reader(fs, new Path(superObj), new Configuration());
			KeyWritable kw = new KeyWritable();
			ValueWritable vw = new ValueWritable();
			reader.seek(offset);
			reader.next(kw, vw);
			if(kw.getObjId() == objId) {
				bytes = vw.getValue();
			}else{
				LOG.error("Read small object mismatch excepted object id " + kw.getObjId()
						+", actual object id " + objId);
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
