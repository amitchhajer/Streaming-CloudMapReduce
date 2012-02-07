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
package com.acnlabs.CloudMapReduce.performance;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class PerformanceTracker {

	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.performance.PerformanceTracker");

	private class Timer {
		
		long num = 0;
		long sumTime = 0;
		long curTime;
		
		public Timer() {
		}
		
		/*public void Start() {
			curTime = System.currentTimeMillis();
		}
		
		public void Stop() {
			sumTime += System.currentTimeMillis() - curTime;
			num++;
		}*/
		
		public String toString() {
			long ave = num > 0 ? sumTime/num : 0;
			return "Num: " + num + ", Ave time: " + ave + ", Total Time: " + sumTime;
		}
	}
	
	private HashMap<String, Timer> timers = new HashMap<String, Timer>();
	private HashMap<String, int[]> counters = new HashMap<String, int[]>();
	
	/*public synchronized void startTimer(String name) {
		Timer timer = getTimer(name);
		timer.Start();
	}
	
	public synchronized void stopTimer(String name) {
		Timer timer = getTimer(name);
		timer.Stop();
	}*/
	
	private Timer getTimer(String name) {
		Timer timer;
		if (!timers.containsKey(name)) {
			timer = new Timer();
			timers.put(name, timer);
		}
		else {
			timer = timers.get(name);
		}
		return timer;
	}
	
	public synchronized long getStartTime() {
		return System.currentTimeMillis();
	}
	
	public synchronized void stopTimer(String name, long startTime) {
		Timer timer = getTimer(name);
		timer.num++;
		timer.sumTime+=System.currentTimeMillis()-startTime;
	}
	
	public synchronized void incrementCounter(String name, int amount) {
		int[] counter;
		if (!counters.containsKey(name)) {
			counter = new int[] {0};
			counters.put(name, counter);
		}
		else {
			counter = counters.get(name);
		}
		counter[0]+=amount;
	}
	
	public void report() {
		for (Entry<String, Timer> entry : timers.entrySet()) {
			logger.info("Name: " + entry.getKey() + ", " + entry.getValue());
		}
		for (Entry<String, int[]> entry : counters.entrySet()) {
			logger.info("Name: " + entry.getKey() + ", amount: " + entry.getValue()[0]);
		}
	}
}
