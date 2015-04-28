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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

public class FileUtil {

	public static void deleteFile(File file) {
		if (file.isFile()) {
			file.delete();
		} else if (file.isDirectory()) {
			for (File f : file.listFiles())
				f.delete();
			file.delete();
		}
	}

	public static void copyFile(File source, File dest) throws IOException {
		FileChannel inputChannel = null;
		FileChannel outputChannel = null;
		try {
			inputChannel = new FileInputStream(source).getChannel();
			outputChannel = new FileOutputStream(dest).getChannel();
			outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
		} finally {
			inputChannel.close();
			outputChannel.close();
		}
	}

	public static void deleteOriginalFile(File dir,String... files) {
		for(String file: files){
			File deletedFile = new File(dir, file);
			deletedFile.delete();
		}
	}

	/**
	 * Read bytes from a File into a byte[].
	 * 
	 * @param file
	 *            The File to read.
	 * @return A byte[] containing the contents of the File.
	 * @throws IOException
	 *             Thrown if the File is too long to read or couldn't be read
	 *             fully.
	 */
	public static byte[] readBytesFromFile(File file) throws IOException {
		InputStream is = new FileInputStream(file);

		// Get the size of the file
		long length = file.length();

		// You cannot create an array using a long type.
		// It needs to be an int type.
		// Before converting to an int type, check
		// to ensure that file is not larger than Integer.MAX_VALUE.
		if (length > Integer.MAX_VALUE) {
			/*throw new IOException("Could not completely read file "
					+ file.getName() + " as it is too long (" + length
					+ " bytes, max supported " + Integer.MAX_VALUE + ")");*/
		}

		// Create the byte array to hold the data
		byte[] bytes = new byte[(int) length];

		// Read in the bytes
		int offset = 0;
		int numRead = 0;
		while (offset < bytes.length
				&& (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
			offset += numRead;
		}

		// Ensure all the bytes have been read in
		/*if (offset < bytes.length) {
			throw new RuntimeException( "Could not completely read file ");
		}*/

		// Close the input stream and return bytes
		is.close();
		return bytes;
	}

	/**
	 * Writes the specified byte[] to the specified File path.
	 * 
	 * @param theFile
	 *            File Object representing the path to write to.
	 * @param bytes
	 *            The byte[] of data to write to the File.
	 * @throws IOException
	 *             Thrown if there is problem creating or writing the File.
	 */
	public static void writeBytesToFile(File theFile, byte[] bytes)
			throws IOException {
		BufferedOutputStream bos = null;

		try {
			FileOutputStream fos = new FileOutputStream(theFile);
			bos = new BufferedOutputStream(fos);
			bos.write(bytes);
		} finally {
			if (bos != null) {
				try {
					// flush and close the BufferedOutputStream
					bos.flush();
					bos.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	public static void appendBytesToFile(File theFile, byte[] bytes)
			throws IOException {
		BufferedOutputStream bos = null;

		try {
			FileOutputStream fos = new FileOutputStream(theFile, true);
			bos = new BufferedOutputStream(fos);
			bos.write(bytes);
		} finally {
			if (bos != null) {
				try {
					// flush and close the BufferedOutputStream
					bos.flush();
					bos.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	
	public static void clearFile(File file) throws IOException {
		FileOutputStream writer = new FileOutputStream(file);
		writer.write((new String()).getBytes());
		writer.close();
	}
	
	public static boolean createEmpty(File file) throws IOException{
		return file.createNewFile();
	}

}
