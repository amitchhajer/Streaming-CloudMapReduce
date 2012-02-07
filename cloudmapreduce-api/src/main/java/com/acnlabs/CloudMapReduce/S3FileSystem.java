/*
* Copyright 2009 Accenture. All rights reserved.
*
* Accenture licenses this file to you under the Apache License, 
* Version 2.0 (the "License"); you may not use this file except in 
* compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* @author Huan Liu (huanliu AT cs.stanford.edu)
*/ 
package com.acnlabs.CloudMapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.amazon.s3.AWSAuthConnection;
import com.amazon.s3.GetResponse;
import com.amazon.s3.ListBucketResponse;
import com.amazon.s3.ListEntry;
import com.amazon.s3.Response;
import com.amazon.s3.S3Object;
import com.amazon.s3.Utils;

/**
 * Interface into the S3 file system on Amazon Web Services
 *
 */
public class S3FileSystem {
	AWSAuthConnection conn;
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.S3FileSystem");
	
	public S3FileSystem(String accessKeyId, String secretAccessKey) {
		conn = new AWSAuthConnection(accessKeyId, secretAccessKey);
	}
	
	public S3Item getItem(String path) {
		return new S3ItemImpl(path);
	}
	
	private class S3ItemImpl implements S3Item {
		String path;
		String bucket;
		long size = -1;
		
		public S3ItemImpl(String path) {
			int pathStart = path.indexOf('/', 1);
			bucket = path.substring(1, pathStart);
			init(bucket, path.substring(pathStart + 1));
		}
		
		public S3ItemImpl(String bucket, String path, long size) {
			this.size = size;
			init(bucket, path);
		}
		
		public S3ItemImpl(String bucket, String path) {
			init(bucket, path);
		}
		
		private void init(String bucket, String path) {
			this.bucket = bucket;
			if (path != null) {
				this.path = path.replace("_$folder$", "/");
			}
		}
		
		public String getPath() {
			return "/" + bucket + "/" + path;
		}
		
		public String getData() {
			try {
				GetResponse response = conn.get(bucket, path, null);
				if (response.object != null) {
					return new String(response.object.data);
				}
			}
			catch (IOException ex) {
				logger.warn(ex.getMessage());
			}
			return "";
		}
		
		public String getData(long start, long len) {
			try {
				Map metadata = new TreeMap();
				metadata.put("Range", Arrays.asList(new String[] { "bytes=" + start + "-" + (start+len-1) }));
				GetResponse response = conn.get(bucket, path, metadata);
				if (response.object != null) {
					return new String(response.object.data);
				}
			}
			catch (IOException ex) {
				logger.warn(ex.getMessage());
			}
			return "";
		}
		
		public long getSize() {
			if (size < 0) {
				try {
					Response response = new Response(conn.makeRequest("HEAD", bucket, Utils.urlencode(path), null, null));
					size = Long.parseLong(response.connection.getHeaderField("Content-Length"));
				}
				catch(Exception ex) {
				}
			}
			return size;
		}
		
		public boolean isDir() {
			return path == null || path.length() == 0 || path.endsWith("/");
		}
		
		public Collection<S3Item> getChildren(boolean all) {
			List<S3Item> children = new ArrayList<S3Item>();
			try {
				String delimiter = all ? null : "/";
				ListBucketResponse response = conn.listBucket(bucket, path, null, null, delimiter, null);
				for (ListEntry entry : (List<ListEntry>)response.entries) {
					children.add(new S3ItemImpl(bucket, entry.key, entry.size));
				}
			}
			catch (IOException ex) {
				logger.warn(ex.getMessage());
			}
			return children;
		}
		
		public S3Item addDir(String name) {
			if (isDir()) {
				try {
					Response response = conn.put(bucket, path + name + "_$folder$", new S3Object(null, null), null);
					response.connection.getResponseMessage();
				}
				catch (IOException ex) {
					logger.warn(ex.getMessage());
				}
			}
			return new S3ItemImpl(bucket, path + name + "_$folder$");
		}
		
		public S3Item upload(String name, byte[] data) {
			if (isDir()) {
				try {
					Response response = conn.put(bucket, path + name, new S3Object(data, null), null);
					response.connection.getResponseMessage();
				}
				catch (IOException ex) {
					logger.warn(ex.getMessage());
				}
			}
			return new S3ItemImpl(bucket, path + name);
		}
		
		public Collection<S3Item> getChildren() {
			return getChildren(false);
		}
	}
	

}
