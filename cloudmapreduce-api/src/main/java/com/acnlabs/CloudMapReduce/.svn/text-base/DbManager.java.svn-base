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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.lang.Math;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.performance.PerformanceTracker;
import com.acnlabs.CloudMapReduce.util.WorkerThreadQueue;
import com.amazonaws.sdb.*;
import com.amazonaws.sdb.model.*;

/*
 * Manages and orchestrates job status for all nodes
 */

public class DbManager {
	
	static final String dbManagerDomain = "CloudMapReduce";
	// simpleDB has a limit on # of attribute per row, which limits how many stat attribute we put in each request
	static final int MAXSTATPERROW = 250;

	private AmazonSimpleDB service;
	private int numSplits;   // # of input splits, used to tell whether map phase is finished
	private int numReduceQ;  // # of reduce queues (# of master reduce queue entries), used to tell whether reduce phase is finished 
	public PerformanceTracker perf = new PerformanceTracker();
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.DbManager");
    private WorkerThreadQueue workers;
	
    /**
     * Instantiates a JobManager based on the SimpleDB access credentials.
     * 
     * @param accessKeyId access key id
     * @param secretAccessKey secret access key
     */
    public DbManager(String accessKeyId, String secretAccessKey, int numMap, int numReduce) {
    	List<String> domainNames = null;
    	Boolean retry = false;
    	
    	// Initialize simpleDB service
        service = new AmazonSimpleDBClient(accessKeyId, secretAccessKey);
        // create the threads to download list from SimpleDB
        workers = new WorkerThreadQueue(Global.numSDBDomain, "DbManager");
        if ( Global.clientID == 0 )  { // only node 0 creates the domain
        	do {
				try {
					retry = false;
					domainNames = service.listDomains(new ListDomainsRequest()).getListDomainsResult().getDomainName();
				} catch (AmazonSimpleDBException ex) {
					retry = true;
					logger.error("Fail to list domain, will retry: " + ex.getMessage());
					try {Thread.sleep(1000 + (new Random()).nextInt(2000));} catch (Exception ex2) {}
				}
        	} while (retry);
        	if ( !domainNames.contains(dbManagerDomain) )
        		workers.push(new CreateDomainRunnable(dbManagerDomain));
	        for ( int i=0 ; i<Global.numSDBDomain ; i++ ) {   // more domains to achieve high throughput
	        	if ( !domainNames.contains(dbManagerDomain+i) )
	        		workers.push(new CreateDomainRunnable(dbManagerDomain+i));
	        }
	        workers.waitForFinish();
        }
        numSplits = numMap;
        numReduceQ = numReduce;
	}
    
	/**
	 * Sets the status for a task to started
	 * 
	 * @param jobID the job ID.
	 * @param phase the phase or task type.
	 * 
	 * @return taskID the taskID for the started task
	 */
	public void startTask(String jobID, String taskID, String phase) {
		// Set status for task in SimpleDB as 'running'
		updateStatus(jobID, taskID, phase, "running");
	}
	
	/**
	 * Sets the status for a task to completed
	 * 
	 * @param jobID the job ID.
	 * @param taskID the task ID.
	 */
	public void completeTask(String jobID, String taskID, String phase) {
		// Set status for task in SimpleDB as 'complete'
		updateStatus(jobID, taskID, phase, "complete");
	}	
	
	/**
	 * Returns whether or not the phase has completed
	 * 
	 * @param jobID the job ID.
	 * @param phase the phase.
	 * @param minimumTasks the minimum number of tasks needed to continue.
	 */
	public boolean isPhaseComplete(String jobID, String phase, int minimumTasks) {
		// query simpleDB to determine if all tasks in the phase have completed
		int all = getPhaseSize(jobID, phase, null);
		int running = getPhaseSize(jobID, phase, "running");
		logger.debug("Checking " + phase + " phase: " + all + " registered " + running + " running.");
		boolean phaseExists = all >= minimumTasks;
		return phaseExists && running == 0;
	}
	
	/**
	 * Waits for a phase to complete
	 * 
	 * @param jobID the job ID.
	 * @param phase the phase.
	 * @param minimumTasks the minimum number of tasks needed to continue.
	 */
	public void waitForPhaseComplete(String jobID, String phase, int minimumTasks) {
		logger.debug("wait for " + phase + " phase to complete");
		// check jobManager to see if all Reducers have completed
		while (!isPhaseComplete(jobID, phase, minimumTasks)) {
			// wait for reduce phase to complete
			try {
				Thread.sleep((new Random()).nextInt(4000) + 2000);
			}
			catch (Exception e) {
				
			}
		}
	}
	
	/**
	 * Creates a domain to hold the task status
	 * 
	 * @param domainName
	 */
    private class CreateDomainRunnable implements Runnable {
    	private String domainName;
    	
    	public CreateDomainRunnable(String domainName) {
    		this.domainName = domainName;
		}
    	
    	public void run() {
			// Check first to see if the domain is already created
			boolean retry;
			int backoff = 1000;
						
		    // Otherwise create the domain
	        do {
				try {
					retry = false;
		        	logger.debug("Creating domain " + domainName);
		            service.createDomain(new CreateDomainRequest().withDomainName(domainName));
			    } catch (AmazonSimpleDBException ex) {
			    	retry = true;
					logger.error("Fail to create domina " + ex.getMessage());
					try {Thread.sleep(backoff + (new Random()).nextInt(2000));} catch (Exception ex2) {}
					backoff *= 2;
			    }
	        } while (retry);
    	}
	}
	
	/**
	 * Updates the status for a task.
	 * Use exponential backoff for now since this is only done once per phase, could evaluate whether spread across domains
	 * 
	 * @param jobID the job ID
	 * @param taskID the task ID
	 * @param phase the phase a task is executing
	 * @param status the status of the task
	 */
	private void updateStatus(String jobID, String taskID, String phase, String status) {
		Boolean retry;
		int backoff = 1000;
		
		ReplaceableAttribute[] attributes = new ReplaceableAttribute[3];
		attributes[0] = new ReplaceableAttribute("jobid", jobID, true);
		attributes[1] = new ReplaceableAttribute("phase", phase, true);
		attributes[2] = new ReplaceableAttribute("status", status, true);
		
		do {
			retry = false;   // assume success
			try {
				PutAttributesRequest request = new PutAttributesRequest().withItemName(jobID + "_" + taskID).withAttribute(attributes);
				PutAttributesResponse response = service.putAttributes(request.withDomainName(dbManagerDomain));
			} catch (AmazonSimpleDBException ex) {
				logger.warn("Fail to update status. Will retry. " + ex.getMessage());
				retry = true;   // signal need to retry
				try {Thread.sleep(backoff + (new Random()).nextInt(2000));} catch (Exception ex2) {}
				backoff *= 2;
			}
		} while (retry);
	}
	
	/**
	 * Queries the database to return the number of tasks in a phase.
	 * 
	 * @param jobID the job ID
	 * @param phase the phase a task is executing
	 */
	private int getPhaseSize(String jobID, String phase, String status) {
		SelectRequest request = new SelectRequest();
		if ( status == null )
			request.setSelectExpression("select count(*) from " + dbManagerDomain + " where jobid = '" + jobID + "' and phase = '" + phase + "'");
		else
	        request.setSelectExpression("select count(*) from " + dbManagerDomain + " where jobid = '" + jobID + "' and phase = '" + phase + "' and status = '" + status + "'");
        try {
        	SelectResponse response = service.select(request);
            if (response.isSetSelectResult()) {
            	SelectResult  selectResult = response.getSelectResult();
                java.util.List<Item> itemList = selectResult.getItem();
                for (Item item : itemList) {
                    if (item.isSetName() && item.getName().equals("Domain") && item.isSetAttribute()) {
	                    java.util.List<Attribute> attributeList = item.getAttribute();
	                    for (Attribute attribute : attributeList) {
	                        if (attribute.isSetName() && attribute.getName().equals("Count") && attribute.isSetValue()) {
	                        	return Integer.parseInt(attribute.getValue());
	                        }
	                    }
                    }
                }
            }           
        } catch (AmazonSimpleDBException ex) {
        	// no need to do extra, caller either get empty or partial result, either case will retry
        	logger.warn("Fail to get phase size. Caller will retry. " + ex.getMessage());
        }

        return 0;  // should never be here
	}
		
	/**
	 * Clean job from database.
	 * 
	 * @param jobID the job ID
	 */
	public void clean(String jobID) {
		/*
		// cheating, just remove the domain, TODO: change to per jobID in the future
        DeleteDomainRequest request = new DeleteDomainRequest();
        request.setDomainName(dbManagerDomain);
        try {
        	DeleteDomainResponse response = service.deleteDomain(request);
        } catch (AmazonSimpleDBException ex) {
           System.out.println("Caught Exception: " + ex.getMessage());
        }
        */
		Boolean hasNextToken = false;
		String nextToken = null;
		List<String> items = new ArrayList<String>();

		// Get a list of items to delete
		SelectRequest request = new SelectRequest();
		do {
			request.setSelectExpression("select itemName() from " + dbManagerDomain + " where jobid = '" + jobID + "' limit 2500");
			if ( hasNextToken )
				request.setNextToken(nextToken);
			try {
				SelectResponse response = service.select(request);
	            if (response.isSetSelectResult()) {
	            	SelectResult  selectResult = response.getSelectResult();
	                java.util.List<Item> itemList = selectResult.getItem();
	                for (Item item : itemList) {
	                    if (item.isSetName()) {
	                    	items.add(item.getName());
	                    }
	                }
	            }           
			} catch (AmazonSimpleDBException ex) {
	        	// Just cleaning, let it go if fails
	        	logger.warn("Exception in getting list when cleaning db. " + ex.getMessage());
	        }
		} while ( hasNextToken );

		// clean 
		int count = items.size();
		for (String itemName : items) {
			try {
				DeleteAttributesRequest deleteRequest = new DeleteAttributesRequest().withDomainName(dbManagerDomain).withItemName(itemName);
				service.deleteAttributes(deleteRequest);
			} catch (AmazonSimpleDBException ex) {
				logger.warn("Exception in cleaning db. " + ex.getMessage());
			}
		}
		logger.debug("Cleaned " + count + " rows from SimpleDB");
	}

	/**
	 * Task claims responsibility for a reduce key to facilitate deciding which task is the eventual winner
	 * Each reduce task writes one row when it believes there is a conflict
	 * 
	 *                       ClaimedBy                               jobid                     writtenby
	 *  reduce_reduceKey     taskID_receiptHandle                    jobid          
	 *  .......       ......
	 *  
	 * Note that we do not require every reduce to update SimpleDB. This is assuming either the task writes the result once
	 * at the end to the output queue (atomic commit result) or the task does not fail during the processing of a reduce key.
	 * Under these assumptions, there is no need to commit the output results, like we did for Map.
	 * Most MapReduce jobs meet this requirement. However, it one does not, i.e., if a reduce needs to continuously write output data, there are two solutions:
	 * 1. it can write the data to S3, but only commit to the output queue with a pointer to S3 at the very end before it deletes the queue
	 * 2. Since output key should be unique, if there are two results with the same key, one should be discarded. One can consult this table to decide which one
	 * Note that each task is requesting from the Master Reduce queue one at a time, so that it is not possible the task is competing with itself for the same reduce work
	 *  
	 * @param taskID the task ID
	 * @param reduceKey  The reduce key the task is claiming
	 */
	public void claimReduce(String jobID, String taskID, String reduceKey, String receiptHandle) {
		Boolean retry;
		
		long claimReduceTime = perf.getStartTime();

		ReplaceableAttribute[] attributes = new ReplaceableAttribute[3];
		attributes[0] = new ReplaceableAttribute("ClaimedBy", taskID + "_" + receiptHandle, true);
		attributes[1] = new ReplaceableAttribute("jobid", jobID, true);
		attributes[2] = new ReplaceableAttribute("writtenby", jobID+"_"+taskID, true);
		
		do {
			retry = false;   // assume success
			try {
				PutAttributesRequest request = new PutAttributesRequest().withItemName(jobID + "_reduce_"+reduceKey).withAttribute(attributes);
				PutAttributesResponse response = service.putAttributes(request.withDomainName(dbManagerDomain));
			} catch (AmazonSimpleDBException ex) {
				logger.warn("Fail to claim reduce key " + reduceKey + ". Will retry. " + ex.getMessage());
				retry = true;   // signal need to retry
				try {Thread.sleep(1000 + (new Random()).nextInt(2000));} catch (Exception ex2) {}
			}
		} while (retry);
		
		perf.stopTimer("claimReduce", claimReduceTime);
	}

	/**
	 * Return the winning task that claimed the reduceKey
	 * Note the return value includes both the winning taskID and the corresponding receiptHandle to facilitate duplicate deletion. 
	 * 
	 * @param jobID the job ID.
	 * @param reduceKey the reduce key to resolve on
	 */
	public String resolveReduce(String jobID, String reduceKey) {
		Boolean retry;
		String winner = null;
		
		long resolveReduceTime = perf.getStartTime();  // statistics

		do {
			retry = false;   // assume success
			try {
				GetAttributesRequest request = new GetAttributesRequest().withItemName(jobID + "_reduce_"+reduceKey).withAttributeName("ClaimedBy");
				GetAttributesResponse response = service.getAttributes(request.withDomainName(dbManagerDomain));
		        if (response.isSetGetAttributesResult()) {
		            GetAttributesResult  getAttributesResult = response.getGetAttributesResult();
		            java.util.List<Attribute> attributeList = getAttributesResult.getAttribute();
		            for (Attribute attribute : attributeList) {
		                if (attribute.isSetName() && attribute.getName().equals("ClaimedBy") && attribute.isSetValue()) {
							if ( winner == null )
								winner = attribute.getValue();
							else if ( winner.compareTo(attribute.getValue()) < 0 ) {
								logger.info(winner + " lost to " + attribute.getValue());
								// This is an arbitrary resolution function. Can use any function as long as all nodes consistently resolves to the same winner
								winner = attribute.getValue();
							}
		                }
		            }
		        } 
			} catch (AmazonSimpleDBException ex) {
				logger.warn("Failed to resolve reduce key " + reduceKey + ". Will retry. " + ex.getMessage());
				retry = true;   // signal need to retry
				try {Thread.sleep(1000 + (new Random()).nextInt(2000));} catch (Exception ex2) {}
			}
		} while (retry);

		perf.stopTimer("resolveReduce", resolveReduceTime);

		return winner;
	}

	
	// list of committedMap
	private HashSet<String> mapWinners = new HashSet<String>();
	// list of committedReduce
	private HashSet<String> reduceWinners = new HashSet<String>();
	/**
	 * Task commits a Map or Reduce result, this signifies that the results for the map/reduce task is complete
	 * Each map task writes one row whenever it finishes one map key
	 * 
	 *                MapCompletedBy               jobid                     writtenby
	 *  map_mapId     taskID_mapId              jobid          
	 *  .......       ......
	 *  
	 * Similarly, each reduce task writes one row whenever it finishes one reduce key
	 * 
	 *                      ReduceCompletedBy                  jobid                     writtenby
	 *  reduce_reduceId     taskID_reduceId              jobid          
	 *  .......       ......
	 *
	 *  the "CompletedBy" attribute for Map could have multiple attrib/value pairs, thus needs resolution
	 *  
	 * @param taskID the task ID
	 * @param id  either mapId or reduceId
	 * @param stage whether this is map or reduce stage
	 */
	public void commitTask(String jobID, String taskID, int id, Global.STAGE stage) {
		Boolean retry;
		String domainName;
		if ( taskID.hashCode()<0 )
			domainName =  dbManagerDomain + (-taskID.hashCode())%Global.numSDBDomain;
		else 
			domainName =  dbManagerDomain + (taskID.hashCode())%Global.numSDBDomain;
		
		long commitTaskTime = perf.getStartTime();

		ReplaceableAttribute[] attributes = new ReplaceableAttribute[3];
		if ( stage == Global.STAGE.MAP  )
			attributes[0] = new ReplaceableAttribute("MapCompletedBy", taskID + "_map" + id , true);
		else
			attributes[0] = new ReplaceableAttribute("ReduceCompletedBy", taskID + "_reduce" + id , true);
		attributes[1] = new ReplaceableAttribute("jobid", jobID, true);
		attributes[2] = new ReplaceableAttribute("writtenby", jobID+"_"+taskID, true);
		
		do {
			retry = false;   // assume success
			try {
				PutAttributesRequest request;
				if ( stage == Global.STAGE.MAP  )
					request = new PutAttributesRequest().withItemName(jobID + "_map"+id).withAttribute(attributes);
				else
					request = new PutAttributesRequest().withItemName(jobID + "_reduce"+id).withAttribute(attributes);
				PutAttributesResponse response = service.putAttributes(request.withDomainName(domainName));
			} catch (AmazonSimpleDBException ex) {
				if ( stage == Global.STAGE.MAP  )
					logger.warn("Fail to commit map task " + id + ". Will retry " + ex.getMessage());
				else
					logger.warn("Fail to commit reduce task " + id + ". Will retry " + ex.getMessage());
				retry = true;   // signal need to retry
				try {Thread.sleep(1000 + (new Random()).nextInt(2000));} catch (Exception ex2) {}
			}
		} while (retry);
		
		perf.stopTimer("commitTask", commitTaskTime);
	}

	// Synchronized because many threads are adding at the same time
	private synchronized void insertCommittedTaskWinners(HashSet<String> winners, SelectResponse response) {
		if (response.isSetSelectResult()) {
			SelectResult  selectResult = response.getSelectResult();
			java.util.List<Item> itemList = selectResult.getItem();
			for (Item item : itemList) {
				java.util.List<Attribute> attributeList = item.getAttribute();
				String winner = null;
				for (Attribute attribute : attributeList) {
					if (attribute.isSetValue()) {  // only check value since we selected only one attribute name  
						if ( winner == null )
							winner = attribute.getValue();
						else if ( winner.compareTo(attribute.getValue()) < 0 ) {
							logger.info("Invalidating work done by " + winner + " in favor of " + attribute.getValue());
							perf.incrementCounter("discardedMap", 1);   // invalidation only happens for Map, reduce work is never duplicated
							// This is an arbitrary resolution function. Can use any function as long as all nodes consistently resolves to the same winner
							winner = attribute.getValue();
						}
					}
				}
				// add to the overall winner list
				winners.add(winner);
			}
		}           
	}
	
	// a separate thread to query a domain for committedMap
    private class CollectCommittedTaskRunnable implements Runnable {
    	String jobID;
    	int   page;    // the domain number to query
    	Global.STAGE stage;   // collecting for map tasks or reduce tasks?
    	
    	public CollectCommittedTaskRunnable(String jobID, int page, Global.STAGE stage) {
    		this.jobID = jobID;
    		this.page = page;
    		this.stage = stage;
    	}
    	
    	public void run() {
    		Boolean hasNextToken = false;
    		String nextToken = null;
    		
    		long getCommittedTaskTime = perf.getStartTime();  // statistics

    		SelectRequest request = new SelectRequest();
    		do {
    			if (stage == Global.STAGE.MAP)
    				request.setSelectExpression("select MapCompletedBy from " + dbManagerDomain + page + " where jobid = '" + jobID + "' and MapCompletedBy is not null limit 2500");
    			else
    				request.setSelectExpression("select ReduceCompletedBy from " + dbManagerDomain + page + " where jobid = '" + jobID + "' and ReduceCompletedBy is not null limit 2500");
    			if ( hasNextToken )
    				request.setNextToken(nextToken);
    			try {
    				SelectResponse response = service.select(request);
    				if (stage == Global.STAGE.MAP )
    					insertCommittedTaskWinners(mapWinners, response);
    				else
    					insertCommittedTaskWinners(reduceWinners, response);
    			} catch (AmazonSimpleDBException ex) {
    				// no need to do extra, caller either get empty or partial result, either case will retry
    				logger.warn("Caught Exception: " + ex.getMessage());
    			}
    		} while ( hasNextToken );

    		perf.stopTimer("getCommittedTask", getCommittedTaskTime);
    	}
    }

	/**
	 * Return a list of Task,mapId or Task,reduceId pair as a result of duplicate resolution to remove 1) duplicate from eventual consistency (for map only) 2) partial results from failed map/reduce
	 * 
	 * @param jobID the job ID.
	 * @param map  is this for map or reduce?
	 */
	public HashSet<String> getCommittedTask(String jobID, Global.STAGE stage) {
		// clear previous list
		if (stage == Global.STAGE.MAP)
			mapWinners.clear();
		else
			reduceWinners.clear();
		// fire threads to get from multiple domains
		for ( int i=0 ; i<Global.numSDBDomain ; i++ )
    		workers.push(new CollectCommittedTaskRunnable(jobID, i, stage));

		// wait for all threads to finish
		workers.waitForFinish();
		if (stage == Global.STAGE.MAP)
			return mapWinners;
		else
			return reduceWinners;
	}

	/**
	 * Check whether enough map/reduce tasks have committed, so that we know whether map/reduce stage finished
	 * 
	 * @param jobID the job ID.
	 * @param map  map or reduce stage?
	 */
	public Boolean isStageFinished(String jobID, Global.STAGE stage) {
		int processedTask = getCommittedTask(jobID, stage).size();
		if (stage == Global.STAGE.MAP )
			logger.debug("Done " + processedTask + " maps, waiting for " + numSplits);
		else
			logger.debug("Done " + processedTask + " reduces, waiting for " + numReduceQ);
		
		if ( stage == Global.STAGE.MAP  && processedTask > numSplits )
			logger.warn("Processed more maps than available");
		else if ( stage == Global.STAGE.REDUCE && processedTask > numReduceQ )
			logger.warn("Processed more reduces than available");

		if ( stage == Global.STAGE.MAP  )
			return processedTask >= numSplits;
		else
			return processedTask >= numReduceQ;
	}
		
	
	
	
	private int reduceQSize, reduceQCount;
	private int localReduceQId;
	private HashSet<String> localCommittedMap;
	/**
	 * Updates the number of key-value pairs generated for each reduce queue
	 * Each map task writes how many records it has written into each reduce queue
	 * ideally, each task writes one row, but in reality, each task writes several row where each row has less than 256 attributes because of SQS limitation
	 * 
	 *                        jobid    writtenby     reduceQx  reduceQy  reduceQz .......     reduceQx1   reduceQy1  reduceQz1
	 *  jobid_taskid_mapid    jobid    taskID_mapId     123       123      123    .......
 	 *  jobid_taskid_mapid    jobid    taskID_mapId                                             123          123        123    .......
	 *  
	 * @param taskID the task ID
	 * @param mapID the mapId that generated the stats
	 * @param stat array of counts of key-value pairs for each reduce queue 
	 */
	public void updateReduceOutputPerMap(String jobID, String taskID, int mapId, int[] stat) {
		Boolean retry = false;
		int backoff = 1000;
		String domainName;
		if ( taskID.hashCode()<0 )
			domainName =  dbManagerDomain + (-taskID.hashCode())%Global.numSDBDomain;
		else 
			domainName =  dbManagerDomain + (taskID.hashCode())%Global.numSDBDomain;
		
		long updateCompletedTime = perf.getStartTime();

		int size = stat.length > MAXSTATPERROW ? MAXSTATPERROW+2 : stat.length+2;
		ReplaceableAttribute[] attributes = new ReplaceableAttribute[size];
		
		int i = 0;
		do {
			int start = i;
			// add jobID attribute to easily identify and delete if needed
			attributes[0] = new ReplaceableAttribute("jobid", jobID, true);
			// add who updated the attribute for debugging purpose
			attributes[1] = new ReplaceableAttribute("writtenby", taskID + "_map" + mapId, true);
			// SimpleDB limits 256 attributes per item, have to split into multiple rows for large # of Qs
			for (  ; i<stat.length && i<start+MAXSTATPERROW ; i++ ) {
				attributes[i % MAXSTATPERROW + 2] = new ReplaceableAttribute("reduceQ" + i, Integer.toString(stat[i]), true);
			}

			// upload to SimpleDB
			do {
				retry = false;   // assume success
				try {
					// row name contains jobID so that multiple jobs could run at the same time without violating the 256 attributes per row limit
					PutAttributesRequest request = new PutAttributesRequest().withItemName(jobID + "_" + taskID + "_map" + mapId + "_" + Math.round(i/MAXSTATPERROW)).withAttribute(attributes);
					PutAttributesResponse response = service.putAttributes(request.withDomainName(domainName));
				} catch (AmazonSimpleDBException ex) {
					logger.warn("Fail to update completed reduce. Will retry. " + ex.getMessage());
					retry = true;   // signal need to retry
					try {Thread.sleep(backoff + (new Random()).nextInt(2000));} catch (Exception ex2) {}
					backoff *= 2;
				}
			} while (retry);
			
		} while ( i < stat.length ) ;

		perf.stopTimer("dbUpdateCompletedReduce", updateCompletedTime);
	}

	// Synchronized because many threads are adding at the same time
	private synchronized void addReduceQSize(SelectResponse response) {
		Boolean validcount;
		int localsize;
        if (response.isSetSelectResult()) {
        	localsize = 0;
        	SelectResult  selectResult = response.getSelectResult();
            java.util.List<Item> itemList = selectResult.getItem();
            for (Item item : itemList) {
                java.util.List<Attribute> attributeList = item.getAttribute();
                validcount = false;
                for (Attribute attribute : attributeList) {
                    if (attribute.isSetName() && attribute.getName().equals("writtenby") && attribute.isSetValue() && localCommittedMap.contains(attribute.getValue()))
                    	validcount = true;   // This is a valid committed map count
                    if (attribute.isSetName() && attribute.getName().equals("reduceQ" + localReduceQId) && attribute.isSetValue())
                    	localsize = Integer.parseInt(attribute.getValue());
                }
                if (validcount) {
                	reduceQSize += localsize;
                	reduceQCount ++ ;
                }
            }
        }           
	}
	
	// a separate thread to query a domain for committedMap
    private class CollectReduceQSizeRunnable implements Runnable {
    	String jobID;
    	int   page;    // the domain number to query
    	
    	public CollectReduceQSizeRunnable(String jobID, int page) {
    		this.jobID = jobID;
    		this.page = page;
    	}
    	
    	public void run() {
    		Boolean hasNextToken = false;
    		String nextToken = null;

    		long getReduceQSizeTime = perf.getStartTime();
    	
    		SelectRequest request = new SelectRequest();
    		do {
    			request.setSelectExpression("select writtenby, reduceQ" + localReduceQId + " from " + dbManagerDomain + page + " where jobid = '" + jobID + "' and reduceQ" + localReduceQId + " is not null limit 2500");
    			if ( hasNextToken )
    				request.setNextToken(nextToken);
    			try {
    				SelectResponse response = service.select(request);
    				addReduceQSize(response);   // call central function to tally
    		        if (response.isSetSelectResult()) {
	    				SelectResult  selectResult = response.getSelectResult();
	    				// Need to continue to query?
	    				if ( selectResult.isSetNextToken() ) {
	    					hasNextToken = true;
	    					nextToken = selectResult.getNextToken();
	    				}
    		        }
    			} catch (AmazonSimpleDBException ex) {
    				// TODO, add retries
    				logger.error("Fail to get reduce queue size. Should retry. " + ex.getMessage());
    			}
    		} while (hasNextToken);  // continue if has more data to analyze
    		perf.stopTimer("dbGetReduceQSize", getReduceQSizeTime);
    	}
    }
    		
   /**
	 * Get the total number of items in a reduce queue
	 * MapReduce class makes sure only call this function one at a time
	 * 
	 * @param jobID the jobID
	 * @param reduceQId the numerical id of the reduce queue
	 */
	public int getReduceQSize(String jobID, int reduceQId, HashSet<String> committedMap) {
		localCommittedMap = committedMap; 
		localReduceQId = reduceQId;
		do {
			reduceQSize = 0;
			reduceQCount = 0;
			
			// fire threads to get from multiple domains
			for ( int i=0 ; i<Global.numSDBDomain ; i++ )
	    		workers.push(new CollectReduceQSizeRunnable(jobID, i));
	
			// wait for all threads to finish
			workers.waitForFinish();

			if ( reduceQCount < numSplits ) {
				logger.debug("Getting reduceQ " + reduceQId + " size, only " + reduceQCount + " out of " + numSplits + " found. Will retry.");
				try {Thread.sleep(1000);} catch (Exception ex2) {}
			}
		} while ( reduceQCount < numSplits );
		
		return reduceQSize;
	}

	
}
