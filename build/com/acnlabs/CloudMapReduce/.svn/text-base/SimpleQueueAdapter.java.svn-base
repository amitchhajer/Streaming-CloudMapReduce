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

import com.amazonaws.queue.model.Message;

public class SimpleQueueAdapter extends SimpleQueue {
	private SimpleQueue queue;
	
	public SimpleQueueAdapter(SimpleQueue queue) {
		this.queue = queue;
	}
	
	public boolean create() {return queue.create();}
	public boolean exists() {return queue.exists();}
	public String getName() {return queue.getName();}
	public void push(String value) {queue.push(value);}
	public boolean hasNext() {return queue.hasNext();}
	public Message next() {return queue.next();}
	public void remove() {queue.remove();}
    public void deleteMessage(String receiptHandle) { queue.deleteMessage(receiptHandle); }  
    public void flush() { queue.flush(); }
}
