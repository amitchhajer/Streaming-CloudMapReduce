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
package com.acnlabs.CloudMapReduce.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.DbManager;
import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.QueueManager;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.performance.PerformanceTracker;
import com.acnlabs.CloudMapReduce.util.WorkerThreadQueue;
import com.amazonaws.queue.model.Message;

/**
 * The main function that implements MapReduce
 */
public class MapReduce {
	
	private DbManager dbManager;
	private QueueManager queueManager;
	private PerformanceTracker perf = new PerformanceTracker();
	private WorkerThreadQueue mapWorkers;
	private WorkerThreadQueue reduceWorkers;
	private WorkerThreadQueue queueWorkers;  // used for input/output/master reduce queues
	private String jobID;
	private String taskID;
	private SimpleQueue inputQueue;
	private SimpleQueue outputQueue;
	private SimpleQueue masterReduceQueue;
	private HashSet<String> committedMap;  // set of taskId,mapId pair that generated valid reduce messages
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.MapReduce");
	private int numReduceQs;

	public class MapCollector implements OutputCollector {
		
		// each map collector has its own set of queue to facilitate tagging
		private HashMap<String, SimpleQueue> queues = new HashMap<String, SimpleQueue>();
		private int numReduceQs;
		private int mapId;  // one collector per map to facilitate tagging
		private WorkerThreadQueue workers;
		private int[] reduceQStat;  // one set of stats per mapId

		public MapCollector(int numReduceQs, int mapId) {
			this.numReduceQs = numReduceQs;
			this.mapId = mapId;
			reduceQStat = new int[numReduceQs];
	        workers = new WorkerThreadQueue(Global.numUploadWorkersPerMap, "map"+mapId);
		}
		
		public void collect(String key, String value) throws IOException {
			perf.incrementCounter("mapOutputRecords", 1);
			SimpleQueue reduceQueue;
			int hash = (key.hashCode() & Integer.MAX_VALUE) % numReduceQs;
			
			// keep statistics so we know how many we expect when we read
			reduceQStat[hash] ++ ;
			
			String queueName = getSubReduceQueueName(jobID, String.valueOf(hash));
		
    		if (queues.containsKey(queueName)) {
    			reduceQueue = queues.get(queueName);
    		}
    		else {
    			reduceQueue = queueManager.getQueue(queueName, true, 1, QueueManager.QueueType.REDUCE, null, taskID + "_map" + mapId, workers);
    			queues.put(queueName, reduceQueue);
    		}
    		
    		// SQS supports limited Unicode, see http://www.w3.org/TR/REC-xml/#charsets, has to encode both key and value to get around
    		// encode conservatively using UTF-8, someone more familiar with encoding need to revisit
    		// Encode separately so that the separator could take on characters not in UTF-8 but accepted by SQS
    		try {
    			reduceQueue.push(URLEncoder.encode(key, "UTF-8") + Global.separator + URLEncoder.encode(value, "UTF-8"));
    		}
    		catch (Exception ex) {
        		logger.error("Message encoding failed. " + ex.getMessage());
    		}

		}
		
		public void close() {
			// update the reduce queue processed count to SimpleDB 
			dbManager.updateReduceOutputPerMap(jobID, taskID, mapId, reduceQStat);
			try {
		    	for (SimpleQueue queue : queues.values()) {
		    		queue.flush();   // Efficient queues need to be cleared
		    	}
				this.workers.waitForFinish();
				this.workers.close();  // flush the queue and kill the threads
			} catch (Exception e) {
				logger.warn("Fail to close worker: " + e.getMessage());
			}
		}		
	}
	
	public class CombineCollector implements OutputCollector {
		
		private static final long maxCombineMemorySize = 64*1024*1024;
		private Reducer combiner;
		private MapCollector output;
		private HashMap<String, Object> combineStates = new HashMap<String, Object>();
		private long memorySize = 0;
		
		public CombineCollector(Reducer combiner, MapCollector output) {
			this.combiner = combiner;
			this.output = output;     // This is the normal Map collector, write to the reduce queue
		}
		
		public void collect(String key, String value) throws Exception {
			perf.incrementCounter("combineMapOutputRecords", 1);
			
			if (combiner == null) {  // if no combiner, just use Map collector as normal
				output.collect(key, value);
				return;
			}
			if (!combineStates.containsKey(key)) {
				Object state = combiner.start(key, output);
				combineStates.put(key, state);
				memorySize += combiner.getSize(state) + key.length();
			}
			Object state = combineStates.get(key);
			long stateSize = combiner.getSize(state);
			combiner.next(key, value, state, output, perf);
			memorySize += combiner.getSize(state) - stateSize;  // account for the memory increase due to State 
			if (memorySize >= maxCombineMemorySize) {
				flush();
			}
		}
		
		public void flush() {
			if (combiner == null) {
				return;
			}
			for (Entry<String, Object> entry : combineStates.entrySet()) {
				try {
					combiner.complete(entry.getKey(), entry.getValue(), output);
				}
				catch (Exception e) {
					logger.warn("Combiner exception: " + e.getMessage());
				}
			}
			combineStates.clear();
			memorySize = 0;
		}
	}

	public class ReduceCollector implements OutputCollector {

		private SimpleQueue outputQueue;
		
		public ReduceCollector(SimpleQueue outputQueue) {
			this.outputQueue = outputQueue;
		}
		
		synchronized public void collect(String key, String value) throws IOException {
			perf.incrementCounter("reduceOutputRecords", 1);
			
			// collect final key-value pair and put in output queue 
			// enqueue to outSQS
    		try {
    			outputQueue.push(URLEncoder.encode(key, "UTF-8") + Global.separator + URLEncoder.encode(value, "UTF-8"));
    		}
    		catch (Exception ex) {
        		logger.error("Message encoding failed. " + ex.getMessage());
    		}
		}
	}
	
	/**
	 * Instantiate MapReduce class by initializing amazon cloud authentication
	 * 
	 * @param dbManager SimpleDB interface to manage job progress
	 * @param queueManager SQS interface to manager I/O
	 */
	public MapReduce(String jobID, DbManager dbManager, QueueManager queueManager, SimpleQueue inputQueue, SimpleQueue outputQueue) {
		this.jobID = jobID;
		this.dbManager = dbManager;
		this.queueManager = queueManager;
		// initialize thread pool for map and reduce workers
		this.mapWorkers = new WorkerThreadQueue(Global.numLocalMapThreads, "mapWorkers");
		this.reduceWorkers = new WorkerThreadQueue(Global.numLocalReduceThreads, "reduceWorkers");
		this.queueWorkers = new WorkerThreadQueue(20, "queueWorkers");  // fix at a constant for now
		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;
	}
	
    private class CreateQueueRunnable implements Runnable {
    	private String jobID;
    	private int reduceQId;
    	
    	public CreateQueueRunnable(String jobID, int reduceQId) {
    		this.jobID = jobID;
    		this.reduceQId = reduceQId;
		}
    	
    	public void run() {
			masterReduceQueue.push(String.valueOf(reduceQId));
			queueManager.getQueue(getSubReduceQueueName(jobID, (String.valueOf(reduceQId))), false, 1, QueueManager.QueueType.REDUCE, null, null, queueWorkers).create();
			logger.debug(reduceQId + ".");
    	}
    }

	// The setup nodes split the task of populating the master reduce queue and creating the reduce queues
	private void setup(int numReduceQs, int numSetupNodes) {
		masterReduceQueue = queueManager.getQueue(getReduceQueueName(jobID), false, 1, QueueManager.QueueType.MASTERREDUCE, null, null, queueWorkers);
		masterReduceQueue.create();
		
		// Setup phase
		long setupTime = perf.getStartTime();
		logger.info("start setup phase");
		
		queueManager.cacheExistingQueues(jobID);
		
		if (Global.clientID < numSetupNodes) {
			String taskID = "setup_" + Global.clientID;
			dbManager.startTask(jobID, taskID, "setup");
			int interval = numReduceQs / numSetupNodes;
			int start = Global.clientID*interval;
			int end = Global.clientID + 1 == numSetupNodes ? numReduceQs : (Global.clientID*interval)+interval;
			
			logger.info("Creating reduce queues: ");
			for (int f = start; f < end; f++) {
				queueWorkers.push(new CreateQueueRunnable(jobID, f));
			}
			queueWorkers.waitForFinish();
//			try {
//				Thread.sleep(60000);  // sleep a little bit to wait
//			} catch (Exception e) {}
			dbManager.completeTask(jobID, taskID, "setup");
		}
		queueWorkers.waitForFinish();
		
		// This is a critical synchronization point, everyone has to wait for all setup nodes 
		// (including the first one, which sets up the input/output queues) finish before proceeding
		dbManager.waitForPhaseComplete(jobID, "setup", numSetupNodes);
		perf.stopTimer("setupTime", setupTime);
	}
	
	// Naming convention for the master reduce queue
	private String getReduceQueueName(String jobID) {
		return jobID + "_reduceQueue";
	}
	// Naming convention for the reduce queues
	private String getSubReduceQueueName(String jobID, String suffix) {
		return jobID + "_reduceQueue" + "_" + suffix;
	}

	// Synchronized access to get ReduceQ size to reduce getReduceQSize implementation complexity
	synchronized int getReduceQSize (String jobID, int bucket, HashSet<String> committedMap) {
		return dbManager.getReduceQSize(jobID, bucket, committedMap);
	}
	
	/**
	 * The MapReduce interface that supports combiner
	 * 
	 * @param map the Map class
	 * @param reduce the Reduce class
	 * @param inputQueue the queue storing inputs 
	 * @param outputQueue the output queue where results go
	 * @param jobID an unique string to identify this MapReduce job. It is used as a prefix to all intermediate SQS queues so that two MapReduce jobs will use different SQS queues for intermediate results.
	 * @param numReduceQs number of reduce queues (size of the master reduce queue)
	 * @param numSetupNodes setup nodes split the task of creating the reduce queues and populating the master reduce queue
	 * @param combiner the Combiner class, typically the same as the Reduce class
	 */
	public void mapreduce(Mapper map, Reducer reduce, 
			 int numReduceQs, int numSetupNodes, Reducer combiner) {
		
		this.numReduceQs = numReduceQs;  // TODO, move it into the constructor??
		
		SimpleQueue masterReduceQueue = queueManager.getQueue(getReduceQueueName(jobID), false, 1, QueueManager.QueueType.MASTERREDUCE, null, null, queueWorkers);
		
		ReduceCollector reduceCollector = new ReduceCollector(outputQueue);

		// Setup
		setup(numReduceQs, numSetupNodes);
		
		// assign task ID
		taskID = "task" + Global.clientID;

		// count overall MapReduce time
		long mapReduceTime = perf.getStartTime();
	
		// MAP phase
		long mapPhase = perf.getStartTime();
		logger.info("start map phase");
		//dbManager.startTask(jobID, taskID, "map");   // log as one map worker 
		
		// read from input SQS
		try {
			do {
				// while input SQS not empty, dequeue, then map
				for (Message msg : inputQueue) {
					String value = msg.getBody();
					// get the key for the key/value pair, in our case, key is the mapId
					int separator = value.indexOf(Global.separator);   // first separator
					int mapId = Integer.parseInt(value.substring(0, separator));
					value = value.substring(separator+Global.separator.length());
					logger.debug(mapId + ".");
					
			   		mapWorkers.push(new MapRunnable(map, combiner, Integer.toString(mapId), value, perf, mapId, msg.getReceiptHandle()));
			   		mapWorkers.waitForEmpty();    // only generate work when have capacity, could sleep here
				}
				// SQS appears to be empty, but really should make sure we processed all in the queue
			} while ( ! dbManager.isStageFinished(jobID, Global.STAGE.MAP) );  // if still has work left in theory, go back bang the queue again
		}
		catch (Exception e) {
        	logger.warn("Map Exception: " + e.getMessage());
		}
		
		// not needed anymore, we wait in getReduceQsize for all counts
		// sync all maps to make sure they finish updateCompletedReduce
		//dbManager.completeTask(jobID, taskID, "map");    
		//dbManager.waitForPhaseComplete(jobID, "map", 0);

		// Get list of valid reduce messages
		committedMap = dbManager.getCommittedTask(jobID, Global.STAGE.MAP);
		
		// stop map phase
		//queueManager.report();
		perf.stopTimer("mapPhase", mapPhase);

		// REDUCE phase
		long reducePhase = perf.getStartTime();
		logger.info("start reduce phase");
		
		try {			
			do {
				// dequeue one reduce key , generate a iterator of values
				for (Message msg : masterReduceQueue) {
					String bucket = msg.getBody();
			   		reduceWorkers.push(new ReduceRunnable(jobID, bucket, reduce, reduceCollector, msg.getReceiptHandle()));
			   		reduceWorkers.waitForEmpty();    // only generate work when have capacity, could sleep here
				}
			} while ( !dbManager.isStageFinished(jobID, Global.STAGE.REDUCE));
			reduceWorkers.waitForFinish();
		}
		catch ( Exception ex ) {
			logger.warn("Reduce Exception: " + ex.getMessage());
		}
		// stop reduce phase
//		dbManager.completeTask(jobID, taskID, "reduce");
//		dbManager.waitForPhaseComplete(jobID, "reduce");
		
		outputQueue.flush();  // output queue is an efficient queue, need to flush to clear
		perf.stopTimer("reducePhase", reducePhase);	
		perf.stopTimer("mapReduce", mapReduceTime);

		queueManager.perf.report();
		perf.report();
		dbManager.perf.report();
	}

    private class MapRunnable implements Runnable {
		private Mapper map;
		private Reducer combiner;
		private String key;
    	private String value;
    	private PerformanceTracker perf;
    	private int mapId;
    	private String receiptHandle;
    	
    	public MapRunnable(Mapper map, Reducer combiner, String key, String value, PerformanceTracker perf, int mapId, String receiptHandle) {
    		this.map = map;
    		this.combiner = combiner;
    		this.key = key;
    		this.value = value;
    		this.perf = perf;
    		this.mapId = mapId;
    		this.receiptHandle = receiptHandle;
		}
    	
    	public void run() {	
    		try {
				long mapLocal = perf.getStartTime();
    			MapCollector mapCollector = new MapCollector(numReduceQs, mapId);
    			CombineCollector combineCollector = new CombineCollector(combiner, mapCollector);

				long mapFunction = perf.getStartTime();     // for profiling purpose
				map.map(key, value, combineCollector, perf);
				perf.stopTimer("mapFunction", mapFunction);
				// make sure all results are saved in SQS
				combineCollector.flush();
				// commit the change made by this map task
				dbManager.commitTask(jobID, taskID, mapId, Global.STAGE.MAP);
				mapCollector.close();   // close worker threads, update stats
				// Delete message from input queue, important to delete after commit in case failure
				inputQueue.deleteMessage(receiptHandle);
				perf.stopTimer("mapLocal", mapLocal);
    		}
			catch (Exception e) {
	        	logger.warn("MapRunnable Exception: " + e.getMessage());
			}
    	}
    }
			
    private class ReduceRunnable implements Runnable {
    	private String jobID;
    	private String bucket;  // the reduce queue number this runnable is responsible for
    	private Reducer reduce;
    	private OutputCollector collector;
    	private HashMap<String, Object> reduceStates;
    	private String receiptHandle;
    	
    	public ReduceRunnable(String jobID, String bucket, Reducer reduce, OutputCollector collector, String receiptHandle) {
    		this.jobID = jobID;
    		this.bucket = bucket;
    		this.reduce = reduce;
    		this.collector = collector;
			this.reduceStates = new HashMap<String, Object>();
			this.receiptHandle = receiptHandle;   // receipt handle for the message in the master reduce queue
		}
    	
    	public void run() {
    		try {
				// total and count are for tracking the number of entries in a reduce queue
				int total = getReduceQSize(jobID, Integer.parseInt(bucket), committedMap);
				int count = 0;
				int oldcount = 0;
				int emptypass = 0;

				// allocate worker
				WorkerThreadQueue workers = new WorkerThreadQueue(Global.numDownloadWorkersPerReduce, "reduce" + bucket);
				// A reduce queue could contain multiple reduce keys
				// set numReduceQReadBuffer high to request a lot to parallelize
				SimpleQueue value = queueManager.getQueue(getSubReduceQueueName(jobID, bucket), true, Global.numReduceQReadBuffer, QueueManager.QueueType.REDUCE, committedMap, null, workers);
				long reduceLocal = perf.getStartTime();
				do {
					for (Message msg : value) {
						String keyVal = msg.getBody();
						count ++ ;
						int sep = keyVal.lastIndexOf(Global.separator);  // could choose indexOf too, should be unique in theory
						String key = keyVal.substring(0, sep);
						String val = keyVal.substring(sep + Global.separator.length());
			    		// decode message as it was encoded when pushed to SQS
			    		try {
			    			key = URLDecoder.decode(key, "UTF-8");
			    			val = URLDecoder.decode(val, "UTF-8");
			    		}
			    		catch (Exception ex) {
			        		logger.error("Message decoding failed. " + ex.getMessage());
			    		}
						if (!reduceStates.containsKey(key)) {
							long reduceStart = perf.getStartTime();
							Object state = reduce.start(key, collector);
							perf.stopTimer("reduceStart", reduceStart);
							reduceStates.put(key, state);
						}
						long reduceNext = perf.getStartTime();
						reduce.next(key, val, reduceStates.get(key), collector, perf);
		                perf.stopTimer("reduceNext", reduceNext);
					}
					if ( oldcount != count ) {
						logger.debug(bucket + ": Processed " + count + " out of total " + total);
						oldcount = count;
						emptypass = 0;
					}
					else 
						emptypass ++ ;
					if ( count < total )
						Thread.sleep(1000);  // sleep a little bit to wait
					if ( emptypass > 5 )  { // should we start conflict resolution process?
						perf.incrementCounter("attemptConflictResolution", 1);   // count how often we do this
						logger.info("start conflict resolution");
						emptypass = 0;   // do not bother again for another round
						dbManager.claimReduce(jobID, taskID, bucket, receiptHandle);
						String winner = dbManager.resolveReduce(jobID, bucket);
						logger.info("resolution winner is " + winner);
						if ( winner != null && ! winner.startsWith(taskID) ) {  // if we are not the winner, clean and quit
							perf.incrementCounter("lostReduceConflictResolution", 1);
							// Do not need this with the current design, only one entry for each number in the master reduce queue
							/*
							String winningReceipt = winner.substring(winner.indexOf('_')+1);
							if ( winningReceipt != receiptHandle ) {
								perf.incrementCounter("lostReduceConflictResolutionBecauseOfDup", 1);
								logger.info("Remove duplicate master reduce message " + receiptHandle);
								// must have been duplicate message in the master reduce queue, remove the redundant one, but not the primary one in case of failure
								masterReduceQueue.deleteMessage(winningReceipt);
							}*/
							return;
						}
					}
				} while ( count < total );  // queue may not be empty because of eventual consistency
				if ( count > total )
					logger.warn("Reduce queue " + bucket + " processed more than available: " + count + " vs. " + total);
				for (Entry<String, Object> entry : reduceStates.entrySet()) {
					long reduceComplete = perf.getStartTime();
					reduce.complete(entry.getKey(), entry.getValue(), collector);
					perf.stopTimer("reduceComplete", reduceComplete);
				}
				// If a reduce task fails before commit, there could be a problem in conflict resolution when others think that this reduce task is still working on it
				// Not a problem in theory because eventually a lower numbered task will grab it, but need to look into faster ways of recovery
				dbManager.commitTask(jobID, taskID, Integer.parseInt(bucket), Global.STAGE.REDUCE);
				masterReduceQueue.deleteMessage(receiptHandle);  // delete what we processed
				// delete threads
				workers.close();  // No need to wait for finish because we only download. The fact that we have downloaded all data means the workers have finished
				// reduceStates.clear();
				perf.stopTimer("reduceLocal", reduceLocal);
    		}
			catch (Exception e) {
	        	logger.warn("ReduceRunnable Exception: " + e.getMessage());
			}
    	}
    }

}
