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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.*;   // for argument processing

import com.acnlabs.CloudMapReduce.DbManager;
import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.QueueManager;
import com.acnlabs.CloudMapReduce.S3FileSystem;
import com.acnlabs.CloudMapReduce.S3Item;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.util.WorkerThreadQueue;
import com.amazonaws.queue.model.Message;

// base class of all MR applications, it initializes the MR process
public abstract class MapReduceApp {
	
	protected DbManager dbManager;
	protected QueueManager queueManager;
	protected S3FileSystem s3FileSystem;
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.MapReduceApp");

	// define accepted arguments
    @Option(name="-k",usage="Amazon Access Key ID")
	protected String accessKeyId = null;      // default is null, so we can check whether user supplied credential

    @Option(name="-s",usage="Amazon Secret Access Key")
	protected String secretAccessKey = null;  // default is null, so we can check whether user supplied credential

    @Option(name="-i",usage="Input: path to S3 bucket&folder for the input files, e.g., /bucket/folder/")
    protected String s3Path = null;

    @Option(name="-j",usage="jobID: used as prefix for all queue names to be unique")
    protected String jobID = "CloudMapReduce";

    // threads are used to parallelize in order to hide S3 and SQS download latency
    @Option(name="-tm",usage="number of local Map threads")
    protected int numLocalMapThreads = 2;

    // threads are used to parallelize in order to hide S3 and SQS download latency
    @Option(name="-tr",usage="number of local Reduce threads")
    protected int numLocalReduceThreads = 4;

	@Option(name="-tu",usage="Number of upload workers for each map")
	protected int numUploadWorkersPerMap = 20;

	@Option(name="-td",usage="Number of download workers for each reduce queue")
	protected int numDownloadWorkersPerReduce = 20;
	
    // reduce queue read buffer size, used to parallelize reading from individual reduce queues
    @Option(name="-b",usage="size of buffer (num of messages) for reading from reduce queues")
    protected int numReduceQReadBuffer = 100;

    // input files are split into equal partitions to facilitate parallel processing
    @Option(name="-p",usage="number of partitions for input files")
    protected int numSplits = 1;

    // reduce work is allocated on reduce queue granularity, more reduce queues =  finer granularity of work allocation
    @Option(name="-r",usage="number of reduce queues (size of master reduce queue)")
    protected int numReduceQs = 1;

    // The setup work (populating input queue, set up master reduce queue) is split evenly among setup nodes
    @Option(name="-n",usage="number of nodes participating in the setup phase")
    protected int numSetupNodes = 1;

    @Option(name="-m",usage="number of SimpleDB domains to use to improve write throughput")
    protected int numSDBDomain = 1;

    @Option(name="-o",usage="name of the output queue to hold results")
    protected String outputQueueName = "outputqueue";

    @Option(name="-c",usage="clientID: this node's number (used to determine if part of setup nodes)")
    protected int clientID = -1;

    @Option(name="-cb",usage="whether combiner is enabled or not)")
    protected boolean enableCombiner = false;

    @Option(name="-d",usage="number of outputs to display for verification purpose only")
    protected int numDisplay = 0;

	@Option(name="-vi",usage="Visibility timeout for the input queue")
	protected int mapQTimeout = 7200;  // timeout for pickup failed map tasks
	
	@Option(name="-vr",usage="Visibility timeout for the reduce queue")
	protected int reduceQTimeout = 600;  // timeout to resume after conflict resolution

    @Option(name="-vm",usage="Visibility timeout for the master reduce queue")
    protected int masterReduceQTimeout = 7200;  // timeout for pickup failed reduce tasks

    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();

	protected abstract void run(String jobID, int numReduceQs, int numSetupNodes, 
			SimpleQueue inputQueue, SimpleQueue outputQueue, int numReduceQReadBuffer) throws IOException;
	
	/**
	 * @param args
	 */
	protected void runMain(String[] args) throws Exception {
		CmdLineParser parser = new CmdLineParser(this);

		try {
			// parse the arguments.
			parser.parseArgument(args);
		} catch( CmdLineException e ) {
			// get exception if has command line problem
			System.err.println(e.getMessage());
			System.err.println("java MapReduceApp [options...]");
			// print the list of available options
			parser.printUsage(System.err);
			System.err.println();
			return;
		}

		// check for mandatory "options"
		if ( accessKeyId == null  || secretAccessKey == null || s3Path == null || clientID == -1 ) {
			System.err.println("-k -s -i -c are mandatory options");
			System.err.println("java MapReduceApp [options...]");
			// print the list of available options
			parser.printUsage(System.err);
			System.err.println();
			return;
		}
				
		// set the global variables
		Global.clientID = clientID;
		Global.numLocalMapThreads = numLocalMapThreads;
		Global.numLocalReduceThreads = numLocalReduceThreads;
		Global.numReduceQReadBuffer = numReduceQReadBuffer;		
		Global.numUploadWorkersPerMap = numUploadWorkersPerMap;
		Global.numDownloadWorkersPerReduce = numDownloadWorkersPerReduce;
		Global.mapQTimeout = mapQTimeout;
		Global.reduceQTimeout = reduceQTimeout;
		Global.masterReduceQTimeout = masterReduceQTimeout;
		Global.enableCombiner = enableCombiner;
		Global.numSDBDomain = numSDBDomain;
		Global.outputQueueName = outputQueueName;
		
		dbManager = new DbManager(accessKeyId, secretAccessKey, numSplits, numReduceQs);
		queueManager = new QueueManager(accessKeyId, secretAccessKey);  // workers used to clean queues
		s3FileSystem = new S3FileSystem(accessKeyId, secretAccessKey);
		
		WorkerThreadQueue workers = new WorkerThreadQueue(20, "general");  // TODO, make it a constant for now
		SimpleQueue inputQueue = queueManager.getQueue(jobID + "_inputqueue", false, 1, QueueManager.QueueType.MAP, null, null, workers);
		SimpleQueue outputQueue = queueManager.getQueue(jobID + "_" + outputQueueName, true, 10, QueueManager.QueueType.OUTPUT, null, null, workers);
				
		// node 0 is responsible for initializing input queue (populate with splits) and output queue
		if (clientID == 0) {
			// clean up
			//dbManager.clean(jobID);
			
			// initialize initial queue
			inputQueue.create();
			outputQueue.create();

			ArrayList<S3Item> fileList = new ArrayList<S3Item>();
			for ( int i=0 ; i<1; i++ ) {
			     addDirToList(s3FileSystem.getItem(s3Path), fileList);
			}
			addSplits(fileList, inputQueue, numSplits);
			
			workers.waitForFinish();
			//initQueueManager.close();
		}
		
		// Run MapReduce application, implemented in individual app
		run(jobID, numReduceQs, numSetupNodes, inputQueue, outputQueue, numReduceQReadBuffer);
		
		// node 0 prints out some outputs for debugging purpose, can be disabled in production with -d 0
		if (clientID == 0 && numDisplay > 0) {
			logger.debug("Output result samples: ");
			int f = 0;
			for (Message msg : outputQueue) {
				String output = msg.getBody();
				if (f >= numDisplay) {
					break;
				}
				logger.debug(output);
				f++;
			}
			logger.debug("Cleaning up");
			dbManager.clean(jobID);  
			//queueManager.deleteQueue(outputQueue);
		}

		// delete all reduce queues, have to wait 60 seconds for next job to create new queues 
		if ( clientID == 0 )
			queueManager.clean(jobID, workers);
		// close all threads
		queueManager.close();
	}
	

	private void addDirToList(S3Item item, ArrayList<S3Item> fileList) {
		if (item.isDir()) {

			Collection<S3Item> children = item.getChildren(true);
			
			if (children.size() < 750) {
				for (S3Item child : children) {
					if (!child.isDir()) {
						fileList.add(child);
					}
				}
			}
			else {
				for (S3Item child : item.getChildren()) {
					if (child.isDir()) {
						addDirToList(child, fileList);
					}
					else {
						fileList.add(child);
					}
				}
			}
		}
		else {
			fileList.add(item);
		}
	}
	
	private void addSplits(ArrayList<S3Item> fileList, SimpleQueue queue, int numSplits) {
		StringBuilder sb = new StringBuilder();
		int mapNum = 0;
		
		long totalSize = 0;
		for (S3Item item : fileList) {
			totalSize += item.getSize();
		}
		long splitSize = totalSize / numSplits + 1;
		logger.info("Total input file size: " + totalSize + ". Each split size: " + splitSize);
		
		long currentSize = 0;
		for (S3Item item : fileList) {
			long filePos = 0;
			while (filePos < item.getSize()) {
				long len = Math.min(item.getSize() - filePos, splitSize - currentSize);
				if (sb.length() > 0) {
					sb.append(",");
				}
				sb.append(item.getPath());
				sb.append(",");
				sb.append(filePos);
				sb.append(",");
				sb.append(len);
				filePos += len;
				currentSize += len;
				if (currentSize == splitSize) {
					// prepend mapNum to uniquely identify each message, mapNum is the key, : is the separator
					queue.push(mapNum + Global.separator + sb.toString());
					mapNum ++ ;
					currentSize = 0;
					sb = new StringBuilder();
				}
			}
		}
		if (sb.length() > 0) {
			// prepend mapNum to uniquely identify each message
			queue.push(mapNum + Global.separator + sb.toString());
			mapNum ++ ;
		}
	}

}
