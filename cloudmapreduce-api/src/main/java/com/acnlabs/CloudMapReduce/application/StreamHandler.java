
/*
 * This module is for handling streaming input by RADIX GROUP
 */
package com.acnlabs.CloudMapReduce.application;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.acnlabs.CloudMapReduce.Global;
import com.acnlabs.CloudMapReduce.S3Item;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import com.acnlabs.CloudMapReduce.S3FileSystem;
public class StreamHandler implements Runnable{
	private S3FileSystem s3FileSystem;
	private String s3Path;
	private long numSplit;
	private SimpleQueue inputQueue;
	private ArrayList<String> preProcessedFileList = new ArrayList<String>();
	private Logger  logger = Logger.getLogger("com.acnlabs.CloudMapReduce.MapReduceApp");
	private static int mapNum;
	public StreamHandler(String s3Path,SimpleQueue inputQueue,long numSplit,S3FileSystem s3FileSystem)
	{
		this.s3Path=s3Path;
		this.inputQueue=inputQueue;
		this.numSplit=numSplit;
		this.s3FileSystem=s3FileSystem;
	}
	public void run()
	{
		while(true)
		{
			try
			{
				ArrayList<S3Item> fileListToBeProcessed = new ArrayList<S3Item>();
				//Dev check for the new file and add to the fileListToBeProcessed
				if(addDirToList(s3FileSystem.getItem(s3Path),fileListToBeProcessed)) 
				{
					for(S3Item child:fileListToBeProcessed)//Dev for testing only
					   logger.info("\n\nNEW FILE FOUND\n\n:" + child.getPath() );
					Global.numSplit+=numSplit;	//to keep track of total number of mappers
					addSplits(fileListToBeProcessed); //Dev add pointer to splits in input queue
				}
				try{
					Thread.sleep(3000);
				}catch(Exception e){
					logger.info("error" + e);
				}
			}catch(Exception e){
			
				logger.info("Error in StreamHangler:" + e);
			}
		}
	}
	private boolean addDirToList(S3Item item,ArrayList<S3Item> fileListToBeProcessed){
		boolean newItem=false;
		if(item == null)
			return newItem;
		if (item.isDir()) {

			Collection<S3Item> children = item.getChildren(true);
			
			if (children.size() < 750) {
				for (S3Item child : children) {
					//Dev if previously processed item then do not add
					if (!child.isDir() && !preProcessedFileList.contains(child.getPath())){
						newItem=true;
						fileListToBeProcessed.add(child);
						/*
						 *  Dev adding child only doesn't work because each request for s3item 
						 *  will give different handle(a number) so added path which will be unique for same item
						 */
						preProcessedFileList.add(child.getPath()); 
					}
				}
			}
			else {
				for (S3Item child : item.getChildren()) {
					if (child.isDir()) {
						addDirToList(child, fileListToBeProcessed);
					}
					else {
						//Dev if previously processed item then do not add
						if(!preProcessedFileList.contains(child.getPath()))
						{
							newItem=true;
							fileListToBeProcessed.add(child);
							preProcessedFileList.add(child.getPath());
						}
					}
				}
			}
		}
		else {
			//Dev if previously processed item then do not add
			if(!preProcessedFileList.contains(item.getPath()))
			{
				newItem=true;
				fileListToBeProcessed.add(item);
				preProcessedFileList.add(item.getPath());
			}
		}
		return newItem; //Dev shows new item is found for processing 
	}
	
	private void addSplits(ArrayList<S3Item> fileListToBeProcessed) {
		StringBuilder sb = new StringBuilder();
		
		long totalSize = 0;
		long splitSize=0;
		for (S3Item item : fileListToBeProcessed) {
			totalSize += item.getSize();
		}
		splitSize=totalSize/numSplit+1;
		logger.info("Total input file size: " + totalSize + ". Each split size: " + splitSize);
		
		long currentSize = 0;
		for (S3Item item : fileListToBeProcessed) {
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
					inputQueue.push(mapNum + Global.separator + sb.toString());
					
					mapNum ++ ;
					currentSize = 0;
					sb = new StringBuilder();
				}
			}
		}
		if (sb.length() > 0) {
			//Dev Prepend mapNum to uniquely identify each message
			inputQueue.push(mapNum + Global.separator + sb.toString());
			mapNum ++ ;
		}
	}
}