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
package com.acnlabs.CloudMapReduce.application;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.S3FileSystem;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.mapreduce.*;
import com.acnlabs.CloudMapReduce.performance.PerformanceTracker;


/**
 * Word Count application
 */
public class WordCount extends MapReduceApp {
	
	private static Logger appLogger = Logger.getLogger("com.acnlabs.CloudMapReduce.application.WordCount");

  /**
   * Counts the words in each line.
   * For each line of input, break the line into words and emit them as
   * (<b>word</b>, <b>1</b>).
   */
  public static class Map implements Mapper { // input: S3 pointer, output: word, count
    
	  S3FileSystem s3;
	
	  
	public Map(S3FileSystem s3) {
		this.s3 = s3;
	}
	  
    public void map(String key, String value, 
                    OutputCollector output, PerformanceTracker perf) // implemented by MapReduce framework 
                    throws Exception {
    	
    	String[] files = value.split(",");

    	for (int f = 0; f < files.length / 3; f++) {
    		String path = files[f*3];
    		long start = Long.parseLong(files[f*3+1]);
    		long len = Long.parseLong(files[f*3+2]);

	    	// download file from S3, parse for words, output to collector
    		
    		appLogger.info("WordCount: download " + path + " start: " + start + " len: " + len);

    		long downloadStart = perf.getStartTime();
	    	String data = s3.getItem(path).getData(start, len);
			perf.stopTimer("wc-S3Download", downloadStart);
			perf.incrementCounter("wc-datasize", data.length());
	
			long wordcountStart = perf.getStartTime();
	    	//String[] words = data.replaceAll("[\\W ]+", " ").split(" ");
			BufferedReader rd = new BufferedReader(new StringReader(data));
			String line = rd.readLine();
			while (line != null) {
		    	String[] words = line.split(" ");
	    	
		    	for (String word : words) {
		    		if (word.length() > 0) {
		        	    //System.out.print(".");
		        	    output.collect(word, "1");
		    		}
		    	}
		    	
		    	line = rd.readLine();

			}
			perf.stopTimer("wc-wordcount", wordcountStart);
    	}
    }
  }
  
  
  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class Reduce implements Reducer<int[]> {
	  
	public int[] start(String key, OutputCollector output) throws IOException {
		return new int[1];
	}
	
	public void next(String key, String value, int[] state, OutputCollector output, PerformanceTracker perf) throws IOException {
		state[0] += Integer.parseInt(value);
	}
	
	public void complete(String key, int[] state, OutputCollector output) throws Exception {
		output.collect(key, String.valueOf(state[0]));
	}
	
	public long getSize(int[] state) throws IOException {
		return 8;
	}
  }  
  
	
	protected void run(String jobID, int numReduceQs,
			int numSetupNodes, SimpleQueue inputQueue, SimpleQueue outputQueue, int numReduceQReadBuffer) 
			throws IOException {
		
		MapReduce mapreduce = new MapReduce(jobID, dbManager, queueManager, inputQueue, outputQueue);
		Map map = new Map(s3FileSystem);
		Reduce reduce = new Reduce();

		// invoke Map Reduce with input SQS name and output SQS name
		if ( Global.enableCombiner )
			mapreduce.mapreduce(map, reduce, numReduceQs, numSetupNodes, reduce);
		else
			mapreduce.mapreduce(map, reduce, numReduceQs, numSetupNodes, null);
	}
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public static void main(String[] args) throws Exception {
	  new WordCount().runMain(args);
	  System.exit(0);
  }

}
