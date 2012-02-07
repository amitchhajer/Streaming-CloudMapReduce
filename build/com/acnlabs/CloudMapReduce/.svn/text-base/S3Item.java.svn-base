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

import java.util.Collection;


public interface S3Item {
	String getPath();
	String getData();
	String getData(long start, long len);
	boolean isDir();
	S3Item addDir(String name);
	S3Item upload(String name, byte[] data);
	Collection<S3Item> getChildren();
	Collection<S3Item> getChildren(boolean all);
	long getSize();
}
