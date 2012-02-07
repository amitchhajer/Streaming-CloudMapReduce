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
package com.acnlabs.CloudMapReduce.examples;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.S3FileSystem;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.application.MapReduceApp;
import com.acnlabs.CloudMapReduce.mapreduce.MapReduce;
import com.acnlabs.CloudMapReduce.mapreduce.Mapper;
import com.acnlabs.CloudMapReduce.mapreduce.OutputCollector;
import com.acnlabs.CloudMapReduce.mapreduce.Reducer;
import com.acnlabs.CloudMapReduce.performance.PerformanceTracker;

public class Grep extends MapReduceApp {
	
	private static String grepPattern;

	public static class Map implements Mapper {
		S3FileSystem s3;
		Pattern pattern;
  
		public Map(S3FileSystem s3) {
			this.s3 = s3;
			pattern = Pattern.compile("(.*" + grepPattern + ".*)");
		}
		
		@Override
		public void map(String key, String value, OutputCollector output, PerformanceTracker perf)
				throws Exception {
			
	    	String[] files = value.split(",");

	    	for (int f = 0; f < files.length / 3; f++) {
	    		String path = files[f*3];
	    		long start = Long.parseLong(files[f*3+1]);
	    		long len = Long.parseLong(files[f*3+2]);
			
	    	    System.out.print(".");
	    	    long downloadStart = perf.getStartTime();
				String data = s3.getItem(path).getData(start, len);
	    	    perf.stopTimer("grep-S3Download", downloadStart);
	    		perf.incrementCounter("grep-datasize", data.length());
				
	    	    long findpattern = perf.getStartTime();
				Matcher matcher = pattern.matcher(data);
				
				while (matcher.find()) {
					String text = matcher.group(1);
					output.collect(String.valueOf(text.hashCode()), text);
				}
	    	    perf.stopTimer("grep-findpattern", findpattern);
	    	}
		}
	}
	
	public static class Reduce implements Reducer<String> {
		
		@Override
		public String start(String key, OutputCollector output)
				throws IOException {
			return "";
		}
		
		@Override
		public void next(String key, String value, String state,
				OutputCollector output, PerformanceTracker perf) throws Exception {
			output.collect(null, value);
		}
		
		@Override
		public void complete(String key, String state, OutputCollector output)
				throws IOException {
		}
		
		public long getSize(String state) throws IOException {
			return state.length();
		}
	}
	
	@Override
	protected void run(String jobID, int numReducers,
			int numSetupNodes, SimpleQueue inputQueue, SimpleQueue outputQueue, int numReduceQReadBuffer)
			throws IOException {
		
		MapReduce mapreduce = new MapReduce(jobID, dbManager, queueManager, inputQueue, outputQueue);
		  Map map = new Map(s3FileSystem);
		  Reduce reduce = new Reduce();

		// invoke Map Reduce with input SQS name and output SQS name
		if ( Global.enableCombiner )
			mapreduce.mapreduce(map, reduce, numReducers, numSetupNodes, reduce);
		else
			mapreduce.mapreduce(map, reduce, numReducers, numSetupNodes, null);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		grepPattern = args[args.length-1];
		new Grep().runMain(args);
		System.exit(0);
	}

}
