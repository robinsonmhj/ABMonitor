/**
 * 
 */
package com.tmg.Log;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Haojie Ma
 * @date Nov 4, 2015
 */
public class FileContainer<T> {

	
	private Queue<T> processingQueue;
	
	
	public FileContainer(){
		processingQueue= new LinkedList<T>();
	}
	
	
	public FileContainer(Queue<T> processingQueue){
		this.processingQueue= processingQueue;
	}
	

	
	public synchronized void add2Queue(T fileName){
		
		processingQueue.add(fileName);
		
	
	}
	
	
	public synchronized  T getFileFromQueue(){	
		if(processingQueue.isEmpty())
			return null;
		else
			return processingQueue.remove();

	}


	


}


