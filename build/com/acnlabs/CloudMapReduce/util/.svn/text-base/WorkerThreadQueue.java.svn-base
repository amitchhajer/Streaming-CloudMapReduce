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
package com.acnlabs.CloudMapReduce.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class WorkerThreadQueue implements Closeable {
	
	private Thread[] threads;
	private List<Runnable> queue = new ArrayList<Runnable>();
	private boolean open = true;
	private int totalAdd = 0;
	private int totalExec = 0;
	private int numThreads;
	private String name;  // name of the thread pool, used for debugging purpose
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.WorkerThreadQueue");
	
	public WorkerThreadQueue(int numThreads, String name) {
		this.name = name;
		this.numThreads = numThreads;
		threads = new Thread[numThreads];
		for (int f = 0; f < threads.length; f++) {
			threads[f] = new Thread(new Runnable() {
				public void run() {
					while (open) {
						Runnable runnable = pop();   // could sleep here waiting for work
						if (runnable != null) {
							runnable.run();
							incExec();
						}
					}
				};
			}, name+"_thread"+f
			);
			
			threads[f].start();
		}
	}
	
	private synchronized void incExec() {
		totalExec++;
	}
	
	public synchronized void push(Runnable runnable) {
		queue.add(runnable);
		if ( totalAdd - totalExec < numThreads )  // some threads are sleep waiting?
			// Will wake up exactly one consumer thread because we are only adding one
			// Will not wake up the producer (waitForEmpty thread), because Mapper and Reducer would not call push and waitForEmpty at the same time
			notify();			
		totalAdd++;
	}
	
	public void waitForFinish() {
		int percentComplete = 0;
		int startTotal = totalExec;
		logger.debug("Waiting for queue workers " + name + " to finish....");
		while(queue.size() > 0 || totalExec < totalAdd) {
			try { Thread.sleep(3000); } catch (Exception ex) {}
			int newPercentComplete = ((totalExec - startTotal) * 100) / (totalAdd - startTotal);
			if (newPercentComplete != percentComplete) {
				logger.debug(name + ": " + newPercentComplete + "% complete");
			}
			percentComplete = newPercentComplete;
		}
		logger.debug("Work queue " + name + ": " + totalAdd + " added, " + totalExec + " executed.");
	}
	
	private synchronized Runnable pop() {
		// notify producer that we are hungry for work
		// could also wake up consumers, but this seems the most clean race-free way 
		if ( queue.size() == 0 )
			notifyAll();
		while ( queue.size() == 0 ) 
			try { 
				wait();     // wait for work		
				if ( !open )
					return null;   // exit to kill the thread if closed
			}
			catch (Exception e) {
	        	logger.warn(name + " worker interrupted unexpectedly: " + e.getMessage());
			}
		if (queue.size() > 0) {
			Runnable runnable = queue.get(0);
			queue.remove(0);
			return runnable;
		}
		return null;
	}
	
	// must call waitForFinish first before close
	public synchronized void close() throws IOException {  // synchronized so that it can wake up everyone else
		open = false;
		// wake up if any is sleeping so that they can terminate their threads
		notifyAll();
	}
	
	// returns if any thread is idle, waiting for work. Used by producer to know when to pump more work.
	public synchronized void waitForEmpty() {
		if ( totalAdd - totalExec < numThreads )  // already have idle threads?
			return;                               // return to let producer generate work
		try {
			wait();                // wait for threads to be idle
		}
		catch (Exception e) {
        	logger.warn(name + " worker interrupted unexpectedly: " + e.getMessage());
		}
	}
	
}
