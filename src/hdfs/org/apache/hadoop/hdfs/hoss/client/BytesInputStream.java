package org.apache.hadoop.hdfs.hoss.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSInputStream;

public class BytesInputStream extends FSInputStream {
	ByteArrayInputStream  bis ;
	private long position;
	
	public BytesInputStream(byte[] buffer){
		this.bis = new ByteArrayInputStream(buffer) ;
	}
	
	@Override
	public void seek(long pos) throws IOException {
		  bis.skip(pos) ;
		  this.position = pos + position;
	}

	@Override
	public long getPos() throws IOException {
	      return this.position;
	}

	@Override
	public boolean seekToNewSource(long targetPos) throws IOException {
		return false;
	}

	@Override
	public int read() throws IOException {
	        int value = bis.read();
	        if (value >= 0) {
	          this.position++;
	        }
	        return value;
	}
	
    public void close() throws IOException { bis.close(); }

}
