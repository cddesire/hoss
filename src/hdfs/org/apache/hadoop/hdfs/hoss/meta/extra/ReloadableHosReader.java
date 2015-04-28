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

package org.apache.hadoop.hdfs.hoss.meta.extra;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdfs.hoss.meta.Hos;
import org.apache.hadoop.hdfs.hoss.meta.HosReader;
import org.apache.hadoop.hdfs.hoss.meta.HosWriter;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

/**
 * A hoss reader that can switch between log files at runtime.
 * 
 * This reader is thread-safe.
 */
public class ReloadableHosReader extends AbstractDelegatingHosReader {

	private final ListeningExecutorService executorService;

	private volatile HosReader reader;
	private volatile File currentLogFile;

	/**
	 * Creates a new {@link ReloadableHosReader} from a log file.
	 * 
	 * @param logFile
	 *            The log file to start with.
	 * @param executorService
	 *            An executor service that is used to run reload tasks on.
	 * @return A future that resolves to the sparkey reader once it has loaded
	 *         the log file.
	 */
	public static ListenableFuture<ReloadableHosReader> fromLogFile(
			File logFile, ListeningExecutorService executorService) {
		ReloadableHosReader reader = new ReloadableHosReader(
				executorService);
		return reader.load(logFile);
	}

	private ReloadableHosReader(ListeningExecutorService executorService) {
		checkArgument(executorService != null,
				"executor service must not be null");
		this.executorService = executorService;
	}

	/**
	 * Load a new log file into this reader.
	 * 
	 * @param logFile
	 *            the log file to load.
	 * @return A future that resolves to the sparkey reader once it has loaded
	 *         the new log file.
	 */
	public ListenableFuture<ReloadableHosReader> load(final File logFile) {
		checkArgument(isValidLogFile(logFile));

		return this.executorService
				.submit(new Callable<ReloadableHosReader>() {
					@Override
					public ReloadableHosReader call() {
						switchReader(logFile);
						return ReloadableHosReader.this;
					}
				});
	}

	@Override
	protected HosReader getDelegateReader() {
		return this.reader;
	}

	private boolean isValidLogFile(File logFile) {
		return logFile != null && logFile.exists()
				&& logFile.getName().endsWith(".spl");
	}

	private HosReader createFromLogFile(File logFile) {
		checkNotNull(logFile);
		checkArgument(logFile.exists());
		checkArgument(logFile.getName().endsWith(".spl"));

		File indexFile = Hos.getIndexFile(logFile);
		if (!indexFile.exists()) {
			try {
				HosWriter w = Hos.append(indexFile);
				w.writeHash();
				w.close();
			} catch (IOException ex) {
				throw new ReloadableHossReaderException(
						"couldn't create index file", ex);
			}
		}

		try {
			return new ThreadLocalHosReader(indexFile);
		} catch (IOException ex) {
			throw new ReloadableHossReaderException(
					"couldn't create hoss reader", ex);
		}
	}

	private synchronized void switchReader(File logFile) {
		if (this.currentLogFile != null && this.currentLogFile.equals(logFile)) {
			return;
		}

		HosReader newReader = createFromLogFile(logFile);
		HosReader toClose = this.reader;

		this.currentLogFile = logFile;
		this.reader = newReader;

		/*long keys = reader.getLogHeader().getNumPuts()
				- reader.getLogHeader().getNumDeletes();*/

		if (toClose != null) {
			try {
				toClose.close();
			} catch (IOException ex) {
				throw new ReloadableHossReaderException(
						"couldn't close readable", ex);
			}
		}
	}

	public static class ReloadableHossReaderException extends
			RuntimeException {
		private static final long serialVersionUID = -5030183468367231345L;

		public ReloadableHossReaderException(String msg, Throwable t) {
			super(msg, t);
		}
	}
}
