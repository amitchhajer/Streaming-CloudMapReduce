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

import java.util.Iterator;

import com.amazonaws.queue.model.Message;


/**
 * The SimpleQueue is an abstraction of the queue in SQS
 *
 */
public abstract class SimpleQueue implements Iterable<Message>, Iterator<Message> {
	public abstract String getName();
	public abstract void push(String value);
	public Iterator<Message> iterator() {
		return this;
	}
	public abstract boolean create();
	public abstract boolean exists();
    public abstract void deleteMessage(String receiptHandle);    
    public abstract void flush();
}
