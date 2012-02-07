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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.performance.PerformanceTracker;
import com.acnlabs.CloudMapReduce.util.WorkerThreadQueue;
import com.amazonaws.queue.AmazonSQS;
import com.amazonaws.queue.AmazonSQSClient;
import com.amazonaws.queue.AmazonSQSException;
import com.amazonaws.queue.model.CreateQueue;
import com.amazonaws.queue.model.DeleteMessage;
import com.amazonaws.queue.model.DeleteQueue;
import com.amazonaws.queue.model.ListQueues;
import com.amazonaws.queue.model.ListQueuesResponse;
import com.amazonaws.queue.model.Message;
import com.amazonaws.queue.model.ReceiveMessage;
import com.amazonaws.queue.model.ReceiveMessageResponse;
import com.amazonaws.queue.model.SendMessage;

/*
 * Manages SQS queues
 */

public class QueueManager implements Closeable {
	
	private String accessKeyId;
	private String secretAccessKey;
	// if there is only one service object, all SQS requests seem to be serialized, resulting in slow performance
	// however, if there are more than 250 service objects, we encountered Internal Error. This is a hack, reuse existing service objects
	// which are lazily allocated
	static final int MAXSERVICEOBJ = 100;
	private AmazonSQS service[] = new AmazonSQS[MAXSERVICEOBJ];
	private int serviceptr = 0;     // pointer to track which service object should be used next
	private HashSet<String> existingQueues = new HashSet<String>();
	static final int NUM_END_QUEUE_RETRIES = 3;   // retries, because queue may return empty at end even if still have things left
	public PerformanceTracker perf = new PerformanceTracker();
	static final int MAX_MESSAGE_BODY_SIZE = 7*1024;
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.QueueManager");
	    
    public enum QueueType {MAP, REDUCE, MASTERREDUCE, OUTPUT }
    
    /**
     * Instantiates a QueueManager based on the SQS access credentials.
     * 
     * @param accessKeyId access key id
     * @param secretAccessKey secret access key
     * @param visibilityTimeout sets the visibility timeout
     */
    public QueueManager(String accessKeyId, String secretAccessKey) {
    	// Initialize SQS services
    	this.accessKeyId = accessKeyId;
    	this.secretAccessKey = secretAccessKey;
    	this.service[0] = new AmazonSQSClient(accessKeyId, secretAccessKey);   // initially only has one
    	for ( int i=1 ; i<MAXSERVICEOBJ ; i++)
    		this.service[i] = null;    // lazily allocate the rest 
	}
    
    synchronized private AmazonSQS getSQSService() {
    	serviceptr ++ ;
        if (serviceptr >= MAXSERVICEOBJ) serviceptr = 1;   // wrap around to reuse
		if ( service[serviceptr] == null )
			service[serviceptr] = new AmazonSQSClient(accessKeyId, secretAccessKey);  // lazily allocate
        return service[serviceptr];  
    }
    
    /**
     * Lists SQS queues
     * 
     * @return the queue requested
     */
    private List<String> listQueues(String prefix) {
    	try {
    		ListQueuesResponse response = service[0].listQueues(new ListQueues().withQueueNamePrefix(prefix));
    		return response.getListQueuesResult().getQueueUrl();
    	}
    	catch (AmazonSQSException ex) {
    		logger.error("Exception in list queue: " + ex.getMessage());
    	}
    	return null;
    }
    
    // Keep a list of all existing SQS queues
    public void cacheExistingQueues(String prefix) {
    	try {
       		// Check queue
    		long listQueueStat = perf.getStartTime();
       		ListQueuesResponse response = service[0].listQueues(new ListQueues().withQueueNamePrefix(prefix));
    		perf.stopTimer("listQueueStat", listQueueStat);
       		for (String url : response.getListQueuesResult().getQueueUrl()) {
       			int nameStart = url.lastIndexOf('/');
       			String existingQueue = url.substring(nameStart+1);
        		if (!existingQueues.contains(existingQueue)) {
            		existingQueues.add(existingQueue);
       			}
       		}
    	}
    	catch (AmazonSQSException ex) {
    		logger.error("Exception in caching existing queues: " + ex);
    	}
    }
    
    /**
     * Gets SQS queue
     * Could hide the latency of SQS get by requesting more messages, but SQS supports at most 10 msgs at a time
     * 
     * This could be called by multiple MapReduceRunners, need to synchronize...
     * 
     * @param name the name of the queue
     * @param efficient whether use the efficient queue implementation
     * @param maxNumMessages the number of messages a query receives at a time. 1-10
     * @return the queue requested
     */
    synchronized public SimpleQueue getQueue(String name, boolean efficient, int maxNumMessages, QueueType type, HashSet<String> committedMap, String tag, WorkerThreadQueue workers) {
    	if (efficient) {
    		return new EfficientQueue(new SimpleQueueImpl(name, maxNumMessages, type, committedMap, tag, workers));
    	}
    	else {
        	return new SimpleQueueImpl(name, maxNumMessages, type, committedMap, tag, workers);  // must be either input/output/masterReduce Queue
    	}
    }
    
    /**
     * Deletes a queue
     * 
     * @param name the name of the queue
     */
    private void deleteQueue(String name) {
    	existingQueues.remove(name);
    	try {
    		// Delete queue
    		long deleteQueueStat = perf.getStartTime();
    		service[0].deleteQueue(new DeleteQueue().withQueueName(name));
    		perf.stopTimer("deleteQueueStat", deleteQueueStat);
    	}
    	catch (AmazonSQSException ex) {
			logger.warn("Exception in deleting queues: " + ex.getMessage());
    	}
    }
        
    // should be called by one node only in order not to duplicate effort
    public void clean(String prefix, WorkerThreadQueue threads) {
    	List<String> queues = listQueues(prefix);
		logger.debug("deleting queues: ");
//    	while (queues.size() > 0) {
            for (String str : queues) {
            	String[] urlParts = str.split("/");
            	String name = urlParts[urlParts.length - 1];
            	if (name.startsWith(prefix) && !name.endsWith(Global.outputQueueName)) {
                	logger.debug("cleaning " + name);
                	if (threads != null) {
                    	threads.push(new DeleteQueueRunnable(name));
                	}
                	else {
                		deleteQueue(name);
                	}
            	}
            }       
            if ( threads != null )
            	threads.waitForFinish();
            queues = listQueues(prefix);
//    	}
    }
        
    public void close() throws IOException {
    }
    
    /**
     * Implementation of the Simple Queue which uses that internal service
     *
     */
    private class SimpleQueueImpl extends SimpleQueue {
    	private String name;
    	private List<Message> messages = new ArrayList<Message>();
    	private int maxNumMessages = 1;
    	private int requestsOutstanding = 0;
    	private AmazonSQS localservice;
    	private int sendMsgNum = 0;    // keep track of the unique number we are sending
    	private HashSet<String> receivedMsg = new HashSet<String>();  // keep track of what msg we have received
    	private QueueType type;
    	private HashSet<String> committedMap = null;
    	private String tag = null;
        private WorkerThreadQueue workers;
    	
    	public SimpleQueueImpl(String name, int maxNumMessages, QueueType type, HashSet<String> committedMap, String tag, WorkerThreadQueue workers) {
    		this.name = name;
    		this.maxNumMessages = maxNumMessages;
    		this.localservice = getSQSService();
    		this.type = type;
            this.committedMap = committedMap;
            this.tag = tag;
            this.workers = workers;
		}
    	
    	public String getName() {
    		return name;
    	}
    	
    	// insert downloaded 
    	synchronized public void insertMessagesSync(List<Message> newMessages) {
    		requestsOutstanding -- ;
    		insertMessages(newMessages);
    	}
    	
    	private void insertMessages(List<Message> newMessages) {
    		if ( newMessages == null ) return;
    		
    		for (Message msg : newMessages) {
    			if ( type == QueueType.REDUCE )  {  // only perform duplicate detection for reduce queues
    				if ( committedMap == null )
    					logger.fatal("CommittedMap cannot be empty");
        			String body = msg.getBody();
        			int idx = body.indexOf(':');
	    			String tag = body.substring(0, idx);
	    			String newbody = body.substring(idx+1);
	    			String committer = tag.substring(0, tag.lastIndexOf('_'));
	    			if (!committedMap.contains(committer))
	    				logger.info("Discard un-committed Map result from " + committer);
	    			else if (receivedMsg.contains(tag))
	    				logger.info("Found duplicate message " + tag + " in reduce queue " + name);
	    			else
	    			{
	    				receivedMsg.add(tag);
	    				msg.setBody(newbody);  // change body to stripped down real content
	    				messages.add(msg);
	    			}
    			}
    			else
    				messages.add(msg);
    		}
    	}
    	
    	// no need to synchronize since workers synchronize already and we do not access other shared critical resources
    	public void push(String value) {
    		// truncate for large values
    		String body = value.length() > MAX_MESSAGE_BODY_SIZE ? value.substring(0, MAX_MESSAGE_BODY_SIZE) : value;
    		if ( type == QueueType.REDUCE ) {  // tag reduce queue messages only  
	    		// Add in tagging information to uniquely identify the message
	    		body = tag + "_" + sendMsgNum + ":" + body;  // : (the first one) is a separator
	    		sendMsgNum ++ ;                                                   // increase to make msg num unique
    		}
    		workers.push(new PushRunnable(name, body, localservice));
    	}
    	
    	/**
    	 * Buffering the queue allows hasNext() and next() to work together so that either one can see
    	 * if there are any messages left in the queue
    	 */ 
    	private void bufferQueue() {
    		if ( maxNumMessages <= 10 ) { // get directly if what we want can be get within one request
    			if (messages.size() == 0) {
		    		try {
		        		long getMessageQueueStat = perf.getStartTime();
		    			// receive the messages and store them in a class variable
		    			for (int f = 0; f < NUM_END_QUEUE_RETRIES && messages.size() == 0; f++) {
	 		    			ReceiveMessage request = new ReceiveMessage().withQueueName(name);
			    			ReceiveMessageResponse response = localservice.receiveMessage(request.withMaxNumberOfMessages(maxNumMessages));
			    			insertMessages(response.getReceiveMessageResult().getMessage());
		    			}
		        		perf.stopTimer("getMessageQueueStat", getMessageQueueStat);
		        	}
		        	catch (AmazonSQSException ex) {
		    			logger.warn("Fail buffer SQS queue: " + ex.getMessage());
		        	}
    			}
    		}
    		else {
    			if ( messages.size() + requestsOutstanding*10 < maxNumMessages ) {
    				// use a variable so that it does not loop endlessly threads are clearing at the same time
    				int threadsToLaunch = (maxNumMessages-messages.size()-requestsOutstanding*10 + 10)/10;
    				for ( int i=0 ; i<threadsToLaunch ; i++) {
    					workers.push(new PopRunnable(this, getSQSService()));
		    			synchronized(this) {
		    				requestsOutstanding ++ ;
		    			}
    				}
	    		}
    		}
    	}
    	
    	public boolean hasNext() {
    		bufferQueue();
    		return messages.size() > 0;
    	}
    	
    	synchronized public Message next() {
    		bufferQueue();
    		if (messages.size() > 0) {
    			Message message = messages.get(0);
    			remove();
    			return message;
    		}
    		return null;
    	}
    	
    	// remove the first message from the message buffer
    	public void remove() {
    		if (messages.size() > 0) {
    			// TODO do not delete messages for performance reasons, just delete the queue at the end, 
    			// have to revisit if the job runs longer than the visibility timeout (2hr) 
    			//String receiptHandle = messages.get(0).getReceiptHandle();
    			//workers.push(new DeleteRunnable(name, receiptHandle));
				messages.remove(0);
    		}
    		
    	}
    	
    	public boolean create() {
    		int visibilityTimeout;
    		
        	// check to see if queue already exists
        	if (exists()) {
        		return false;
        	}
        	
        	// add the name to available queues since it either will be created or already exists
        	existingQueues.add(name);

        	if ( type == QueueType.MAP )
        		visibilityTimeout = Global.mapQTimeout;
        	else if ( type == QueueType.MASTERREDUCE )
        		visibilityTimeout = Global.masterReduceQTimeout;
        	else if ( type == QueueType.REDUCE )
        		visibilityTimeout = Global.reduceQTimeout;
        	else 
        		visibilityTimeout = 500;   // default, should not be here
        	
    		long createQueueStat = perf.getStartTime();
        	try {
        		// Create queue
        		localservice.createQueue(new CreateQueue().withQueueName(name).withDefaultVisibilityTimeout(visibilityTimeout));
        		perf.stopTimer("createQueueStat", createQueueStat);
        	}
        	catch (AmazonSQSException ex) {
        		perf.stopTimer("createQueueStat", createQueueStat);
        		// Queue already exists
            	return false;
        	}
        	return true;
    	}
    	
    	public boolean exists() {
        	// check to see if queue already exists
        	if (existingQueues.contains(name)) {
        		return true;
        	}
        	
        	cacheExistingQueues(name);
   			return existingQueues.contains(name);
    	}
    	
        public void deleteMessage(String receiptHandle) {
        	workers.push(new DeleteRunnable(name, receiptHandle));
        }
        
        public void flush() {}  // nothing to flush for simple queues
    }
    
    // Using the Adapter pattern, instead of inheritance, allows us to make any SimpleQueue to be efficient 
    private class EfficientQueue extends SimpleQueueAdapter {
    	private final String delim = "!-!";   // deliminator between sub messages, make sure it is different from the key/value pair separator
    	StringBuilder body = new StringBuilder();
    	int pos = 0;
    	String[] list = new String[0];
    	
    	public EfficientQueue(SimpleQueue queue) {
    		super(queue);
		}
    	
    	public void push(String value) {
    		if (value.length() >= MAX_MESSAGE_BODY_SIZE) {
        		super.push(value);
    			return;
    		}
    		if (body.length() + value.length() + delim.length() > MAX_MESSAGE_BODY_SIZE) {
        		flush();
    		}
    		if (body.length() > 0) {
        		body.append(delim);
    		}
    		body.append(value);
    	}
    	
    	public void flush() {
    		if (body.length() > 0) {
        		super.push(body.toString());
        		body = new StringBuilder();
    		}
    	}
    	
    	private void getNext() {
    		if (list == null || pos >= list.length) {
    			if (super.hasNext()) {
        			list = super.next().getBody().split(delim);
    			}
    			else {
    				list = null;
    			}
    			pos = 0;
    		}
    	}
    	
    	public boolean hasNext() {
    		getNext();
    		/*if (list != null && pos < list.length) {
    			System.out.println(list[pos]);
    		}*/
        	return list != null && pos < list.length;
    	}
    	
    	// SimpleQueue must return Message type to facilitate message deletion, fake here as a message instead of a string
    	public Message next() {
    		Message msg = new Message();
    		getNext();
    		String next = list[pos];
    		pos++;
    		msg.setBody(next);
    		return msg;
    	}
    	
    	public void remove() {
    	}
    }
    
    // TODO: need to add its own per queue AmazonSQS service object???
    private class DeleteRunnable implements Runnable {
    	String queueName;
    	String receiptHandle;
    	
    	public DeleteRunnable(String queueName, String receiptHandle) {
    		this.queueName = queueName;
    		this.receiptHandle = receiptHandle;
		}
    	
    	public void run() {
        	try {
    			// Delete message from queue
	        	long startTime = perf.getStartTime();
    			DeleteMessage request = new DeleteMessage().withQueueName(queueName);
    			service[0].deleteMessage(request.withReceiptHandle(receiptHandle));
        		perf.stopTimer("deleteMessageQueueStat", startTime);
    			//deleteMessages.remove(0);
            }
            catch (AmazonSQSException ex) {
            	// TODO add retry
        		logger.error("Fail to delete message " + receiptHandle + " from queue " + queueName + ": " + ex.getMessage());
            }
    	}
    }
    
    private class PushRunnable implements Runnable {
    	String queueName;
    	String body;
    	// use a separate AmazonSQS object per queue to improve parallel performance
    	AmazonSQS localservice;
    	
    	public PushRunnable(String queueName, String body, AmazonSQS localservice) {
    		this.queueName = queueName;
    		this.body = body;
    		this.localservice = localservice;
		}
    	
    	public void run() {
    		boolean retry;
    		do {
    			retry = false;
		    	try {
		        	long startTime = perf.getStartTime();
		    		// push value to queue
		    		localservice.sendMessage(new SendMessage().withQueueName(queueName).withMessageBody(body));
		        	perf.stopTimer("pushQueueStat", startTime);
		        }
		        catch (AmazonSQSException ex) {
		        	// TODO add retry. I have seen internal error once. if not retry here, caller would not retry it
		        	// it is different from pop, where caller will keep on retrying until they get the count
		    		logger.error("Fail to push message (size=" + body.length() + ") to queue " + queueName + ": " + ex.getMessage());
					retry = true;   // signal need to retry
					try {Thread.sleep(1000 + (new Random()).nextInt(2000));} catch (Exception ex2) {}
		        }
    		} while (retry ==  true);

    	}
    }

    private class PopRunnable implements Runnable {
    	SimpleQueueImpl queue;
    	AmazonSQS localservice;
    	
    	public PopRunnable(SimpleQueueImpl queue, AmazonSQS localservice) {
    		this.queue = queue;
    		this.localservice = localservice;
    	}
    	
    	public void run() {
    		// every run gets 10 (max SQS get limit) into message queue
    		List<Message> messages = null;
    		try {
        		long getMessageQueueStat = perf.getStartTime();
    			// receive the messages and store them in a class variable
    			for (int f = 0; f < NUM_END_QUEUE_RETRIES && (messages == null || messages.size()==0); f++) {
        			ReceiveMessage request = new ReceiveMessage().withQueueName(queue.getName());
        			ReceiveMessageResponse response = localservice.receiveMessage(request.withMaxNumberOfMessages(10));  // make sure maxNumMessages=10
        			messages = response.getReceiveMessageResult().getMessage();
    			}
    			queue.insertMessagesSync(messages);
        		perf.stopTimer("getMessageQueueStat", getMessageQueueStat);
        	}
        	catch (AmazonSQSException ex) {
    			logger.warn("Fail to pop from queue " + queue.getName() + ". Caller will retry. XML: " + ex.getXML() + ". Caused by: " + ex.getCause());  // XML gives more details
    			queue.insertMessagesSync(null);   // insert null message to let caller know we failed
        	}
    	}
    }

    private class DeleteQueueRunnable implements Runnable {
    	String queueName;
    	
    	public DeleteQueueRunnable(String queueName) {
    		this.queueName = queueName;
		}
    	
    	public void run() {
    		deleteQueue(queueName);
    	}
    }
    
}
