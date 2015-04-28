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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.hoss.ds.BufferStacker;
import org.apache.hadoop.hdfs.hoss.ds.IntHashMap;
import org.apache.hadoop.hdfs.hoss.util.Check64bitsJVM;
import org.apache.hadoop.hdfs.hoss.util.StringSerializer;



/**
 * File based Storage of fixed size blocks This class is NOT Thread-Safe
 * 
 */
public class FileBlockStore {
	private static final Log LOG = LogFactory.getLog(FileBlockStore.class);
	/**
	 * Size of block
	 */
	public final int blockSize;
	/**
	 * File associated to this store
	 */
	private File file = null;
	/**
	 * RamdomAccessFile for this store
	 */
	private RandomAccessFile raf = null;
	/**
	 * FileChannel for this store
	 */
	private FileChannel fileChannel = null;
	/**
	 * Support for mmap
	 */
	private boolean useMmap = false;
	/**
	 * Support for Locking
	 */
	private boolean useLock = false;
	/**
	 * ByteBuffer pool
	 */
	private final BufferStacker bufstack;
	/**
	 * In Valid State?
	 */
	private boolean validState = false;
	/**
	 * Callback called when flush buffers to disk
	 */
	private CallbackSync callback = null;
	/**
	 * File Lock
	 */
	private FileLock lock = null;

	/**
	 * Instantiate FileBlockStore
	 * 
	 * @param file
	 *            name of file to open
	 * @param blockSize
	 *            size of block
	 * @param isDirect
	 *            use DirectByteBuffer or HeapByteBuffer?
	 */
	public FileBlockStore(final String file, final int blockSize,
			final boolean isDirect) {
		this(new File(file), blockSize, isDirect);
	}

	/**
	 * Instantiate FileBlockStore
	 * 
	 * @param file
	 *            file to open
	 * @param blockSize
	 *            size of block
	 * @param isDirect
	 *            use DirectByteBuffer or HeapByteBuffer?
	 */
	public FileBlockStore(final File file, final int blockSize,
			final boolean isDirect) {
		this.file = file;
		this.blockSize = blockSize;
		this.bufstack = BufferStacker.getInstance(blockSize, isDirect);
	}

	// ========= Open / Close =========

	/**
	 * Open file for read/write
	 * 
	 * @return true if valid state
	 */
	public boolean open() {
		return open(false);
	}

	/**
	 * Open file
	 * 
	 * @param readOnly
	 *            open for readOnly?
	 * @return true if valid state
	 */
	public boolean open(final boolean readOnly) {
		if (isOpen()) {
			close();
		}
		if (LOG.isDebugEnabled())
			LOG.debug("open(" + file + ")");
		try {
			raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
			fileChannel = raf.getChannel();
			if (useLock)
				lock(readOnly);
		} catch (Exception e) {
			LOG.error("Exception in open()", e);
			try {
				unlock();
			} catch (Exception ign) {
			}
			try {
				fileChannel.close();
			} catch (Exception ign) {
			}
			try {
				raf.close();
			} catch (Exception ign) {
			}
			raf = null;
			fileChannel = null;
		}
		validState = isOpen();
		return validState;
	}

	/**
	 * Close file
	 */
	public void close() {
		mmaps.clear(false);
		try {
			unlock();
		} catch (Exception ign) {
		}
		try {
			fileChannel.close();
		} catch (Exception ign) {
		}
		try {
			raf.close();
		} catch (Exception ign) {
		}
		fileChannel = null;
		raf = null;
		validState = false;
	}

	// ========= Locking ======

	/**
	 * Lock file
	 * 
	 * @throws IOException
	 */
	public boolean lock(final boolean readOnly) throws IOException {
		if (isOpen() && lock == null) {
			lock = fileChannel.lock(0L, Long.MAX_VALUE, readOnly);
			return true;
		}
		return false;
	}

	/**
	 * Unlock file
	 * 
	 * @throws IOException
	 */
	public boolean unlock() throws IOException {
		if (lock != null) {
			lock.release();
			lock = null;
			return true;
		}
		return false;
	}

	// ========= Info =========

	/**
	 * @return size of block
	 */
	public int getBlockSize() {
		return blockSize;
	}

	/**
	 * @return true if file is open
	 */
	public boolean isOpen() {
		try {
			if (fileChannel != null)
				return fileChannel.isOpen();
		} catch (Exception ign) {
		}
		return false;
	}

	/**
	 * @return size of file in blocks
	 * @see #getBlockSize()
	 */
	public int numBlocks() {
		try {
			final long len = file.length();
			final long num_blocks = ((len / blockSize) + (((len % blockSize) == 0) ? 0
					: 1));
			if (LOG.isDebugEnabled())
				LOG.debug("size()=" + num_blocks);
			return (int) num_blocks;
		} catch (Exception e) {
			LOG.error("Exception in sizeInBlocks()", e);
		}
		return -1;
	}

	// ========= Destroy =========

	/**
	 * Truncate file
	 */
	public void clear() {
		if (!validState)
			throw new InvalidStateException();
		try {
			fileChannel.position(0).truncate(0);
			sync();
		} catch (Exception e) {
			LOG.error("Exception in clear()", e);
		}
	}

	/**
	 * Delete file
	 */
	public void delete() {
		close();
		try {
			file.delete();
		} catch (Exception ign) {
		}
	}

	// ========= Operations =========

	/**
	 * set callback called when buffers where synched to disk
	 * 
	 * @param callback
	 */
	public void setCallback(final CallbackSync callback) {
		this.callback = callback;
	}

	/**
	 * Read block from file
	 * 
	 * @param index
	 *            of block
	 * @return ByteBuffer from pool with data
	 */
	public ByteBuffer get(final int index) {
		if (!validState)
			throw new InvalidStateException();
		if (LOG.isDebugEnabled())
			LOG.debug("get(" + index + ")");
		try {
			if (useMmap) {
				final MappedByteBuffer mbb = getMmapForIndex(index, true);
				if (mbb != null) {
					return mbb;
				}
				// Callback to RAF
			}
			final ByteBuffer buf = bufstack.pop();
			fileChannel.position(index * blockSize).read(buf);
			buf.rewind();
			return buf;
		} catch (Exception e) {
			LOG.error("Exception in get(" + index + ")", e);
		}
		return null;
	}

	/**
	 * Write from buf to file
	 * 
	 * @param index
	 *            of block
	 * @param buf
	 *            ByteBuffer to write
	 * @return true if write is OK
	 */
	public boolean set(final int index, final ByteBuffer buf) {
		if (!validState)
			throw new InvalidStateException();
		if (LOG.isDebugEnabled())
			LOG.debug("set(" + index + "," + buf + ")");
		try {
			if (buf.limit() > blockSize) {
				LOG.error("ERROR: buffer.capacity=" + buf.limit()
						+ " > blocksize=" + blockSize);
			}
			if (useMmap) {
				final MappedByteBuffer mbb = getMmapForIndex(index, true);
				if (mbb != null) {
					mbb.put(buf);
					return true;
				}
				// Callback to RAF
			}
			fileChannel.position(index * blockSize).write(buf);
			return true;
		} catch (Exception e) {
			LOG.error("Exception in set(" + index + ")", e);
		}
		return false;
	}

	/**
	 * Alloc a WriteBuffer
	 * 
	 * @param index
	 *            of block
	 * @return WriteBuffer
	 */
	public WriteBuffer set(final int index) {
		if (useMmap) {
			final ByteBuffer buf = getMmapForIndex(index, true);
			if (buf != null) {
				return new WriteBuffer(this, index, useMmap, buf);
			}
		}
		return new WriteBuffer(this, index, false, bufstack.pop());
	}

	/**
	 * Release Read ByteBuffer
	 * 
	 * @param buf
	 *            readed ByteBuffer
	 */
	public void release(final ByteBuffer buf) {
		if (!useMmap)
			bufstack.push(buf);
	}

	/**
	 * Forces any updates to this file to be written to the storage device that
	 * contains it.
	 */
	public void sync() {
		if (!validState)
			throw new InvalidStateException();
		if (useMmap) {
			syncAllMmaps();
		}
		if (fileChannel != null) {
			try {
				fileChannel.force(false);
			} catch (Exception ign) {
			}
		}
		if (callback != null)
			callback.synched();
	}

	public static interface CallbackSync {
		public void synched();
	}

	public static class WriteBuffer {
		private final FileBlockStore storage;
		private final int index;
		private final boolean mmaped;
		private ByteBuffer buf;

		private WriteBuffer(final FileBlockStore storage, final int index,
				final boolean mmaped, final ByteBuffer buf) {
			this.storage = storage;
			this.index = index;
			this.mmaped = mmaped;
			this.buf = buf;
		}

		public ByteBuffer buf() {
			return buf;
		}

		/**
		 * Save and release the buffer
		 * 
		 * @return successful operation?
		 */
		public boolean save() {
			if (mmaped)
				return true;
			final boolean ret = storage.set(index, buf);
			storage.release(buf);
			buf = null;
			return ret;
		}
	}

	// ========= Mmap ===============

	private static final int segmentSize = (32 * 4096); // N_PAGES * PAGE=4KB //
	// 128KB
	@SuppressWarnings("rawtypes")
	private final IntHashMap<BufferReference> mmaps = new IntHashMap<BufferReference>(
			128, BufferReference.class);
	/**
	 * Comparator for write by Idx
	 */
	private Comparator<BufferReference<MappedByteBuffer>> comparatorByIdx = new Comparator<BufferReference<MappedByteBuffer>>() {
		@Override
		public int compare(final BufferReference<MappedByteBuffer> o1,
				final BufferReference<MappedByteBuffer> o2) {
			if (o1 == null) {
				if (o2 == null)
					return 0; // o1 == null & o2 == null
				return 1; // o1 == null & o2 != null
			}
			if (o2 == null)
				return -1; // o1 != null & o2 == null
			final int thisVal = (o1.idx < 0 ? -o1.idx : o1.idx);
			final int anotherVal = (o2.idx < 0 ? -o2.idx : o2.idx);
			return ((thisVal < anotherVal) ? -1 : ((thisVal == anotherVal) ? 0
					: 1));
		}
	};

	/**
	 * Is enabled mmap for this store?
	 * 
	 * @return true/false
	 */
	public boolean useMmap() {
		return useMmap;
	}

	/**
	 * Enable mmap of files (default is not enabled), call before use
	 * {@link #open()}
	 * <p/>
	 * Recommended use of: {@link #enableMmapIfSupported()}
	 * <p/>
	 * <b>NOTE:</b> 32bit JVM can only address 2GB of memory, enable mmap can
	 * throw <b>java.lang.OutOfMemoryError: Map failed</b> exceptions
	 */
	public void enableMmap() {
		if (validState)
			throw new InvalidStateException();
		if (Check64bitsJVM.JVMis64bits()) {
			LOG.info("Enabled mmap on 64bits JVM");
		} else {
			LOG.warn("Enabled mmap on 32bits JVM, risk of: "
					+ "java.lang.OutOfMemoryError: Map failed");
		}
		useMmap = true;
	}

	/**
	 * Enable mmap of files (default is not enabled) if JVM is 64bits, call
	 * before use {@link #open()}
	 */
	public void enableMmapIfSupported() {
		if (validState)
			throw new InvalidStateException();
		useMmap = Check64bitsJVM.JVMis64bits();
		if (useMmap) {
			LOG.info("Enabled mmap on 64bits JVM");
		} else {
			LOG.info("Disabled mmap on 32bits JVM");
		}
	}

	/**
	 * Enable Lock of files (default is not enabled), call before use
	 * {@link #open()}
	 */
	public void enableLocking(boolean isLock) {
		if (validState)
			throw new InvalidStateException();
		if (isLock) {
			useLock = false;
			LOG.info("Disabled Locking in System Property ("
					+ isLock + ")");
		} else {
			useLock = true;
			LOG.info("Enabled Locking");
		}
	}

	private final int addressIndexToSegment(final int index) {
		return (int) (((long) index * blockSize) / segmentSize);
	}

	private final int addressIndexToSegmentOffset(final int index) {
		return (index % (segmentSize / blockSize));
	}

	/**
	 * map a block(4KB) or a segment(256KB)
	 * @param index 
	 *          the index of block
	 * @param useSegments
	 *           whether use segmeng?
	 * @return   the mapping Buffer
	 */
	@SuppressWarnings("unchecked")
	public final MappedByteBuffer getMmapForIndex(final int index,
			boolean useSegments) {
		if (!validState)
			throw new InvalidStateException();
		final int mapIdx = (useSegments ? addressIndexToSegment(index) : index);
		final int mapSize = (useSegments ? segmentSize : blockSize);
		try {
			final Reference<MappedByteBuffer> bref = mmaps.get(mapIdx);
			MappedByteBuffer mbb = null;
			if (bref != null) {
				mbb = bref.get();
			}
			if (mbb == null) { // Create mmap
				final long mapOffset = ((long) mapIdx * mapSize);
				mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE,
						mapOffset, mapSize);
				// mbb.load();
				mmaps.put(mapIdx, new BufferReference<MappedByteBuffer>(mapIdx,
						mbb));
			} else {
				mbb.clear();
			}
			if (useSegments) { // slice segment
				final int sliceBegin = (addressIndexToSegmentOffset(index) * blockSize);
				final int sliceEnd = (sliceBegin + blockSize);
				mbb.limit(sliceEnd);
				mbb.position(sliceBegin);
				mbb = (MappedByteBuffer) mbb.slice();
			}
			return mbb;
		} catch (IOException e) {
			LOG.error("IOException in getMmapForIndex(" + index + ")", e);
		}
		return null;
	}
	@SuppressWarnings("unchecked")
	private void syncAllMmaps() {
		final BufferReference<MappedByteBuffer>[] maps = mmaps.getValues();
		Arrays.sort(maps, comparatorByIdx);
		for (final Reference<MappedByteBuffer> ref : maps) {
			if (ref == null)
				break;
			final MappedByteBuffer mbb = ref.get();
			if (mbb != null) {
				try {
					mbb.force();
				} catch (Exception ign) {
				}
			}
		}
	}

	static class BufferReference<T extends MappedByteBuffer> extends
			SoftReference<T> {
		final int idx;

		public BufferReference(final int idx, final T referent) {
			super(referent);
			this.idx = idx;
		}
	}

	// ========= Exceptions =========

	/**
	 * Exception throwed when store is in invalid state (closed)
	 */
	public static class InvalidStateException extends RuntimeException {
		private static final long serialVersionUID = 42L;
	}

	// ========= END =========

	public static void main(String[] args) {
		final int BLOCK_SIZE = 64;
		final int TOTAL = 10000000;
		final FileBlockStore fbs = new FileBlockStore("./data/block",
				BLOCK_SIZE, false);
		fbs.delete();
		// fbs.enableMmap(); // Test MMAPED?
		fbs.open();
		long start = System.currentTimeMillis();
		for (int i = 0; i < TOTAL; i++) {
			final WriteBuffer wbuf = fbs.set(i);
			final ByteBuffer buf = wbuf.buf();
			StringSerializer.fromStringToBuffer(buf, "hehe" + i);
			buf.putLong(i);
			buf.flip();
			wbuf.save();
		}
		//
		fbs.sync();
		System.out.println("write time: " + (System.currentTimeMillis() - start) / 1000);
		start = System.currentTimeMillis();
		for (int j = 0; j < TOTAL; j++) {
			final ByteBuffer buf = fbs.get(j);
			if (buf == null) {
				System.out.println("Error trying read block " + j + " blocks="
						+ fbs.numBlocks());
				break;
			}
			StringSerializer.fromBufferToString(buf);
			buf.getLong();
			//System.out.print("\t" + hehe + "\t" + offset);
		}
		System.out.println("read time: " + (System.currentTimeMillis() - start) / 1000);
		fbs.close();

	}
}
