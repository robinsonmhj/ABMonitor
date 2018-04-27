/**
 * 
 */
package com.tmg.Log;


import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.tmg.Util.FileUtil;
import com.tmg.greenplum.DAOImp.GreenplumDAOImp;

/**
 * @author Haojie Ma
 * @date Nov 4, 2015
 */
public class ProcessFileThread implements Runnable{

	
	private static Logger log=Logger.getLogger(ProcessFileThread.class);
	
	private FileContainer<String> container;
	private FileContainer<ProcessFile> processFilecontainer;
	private List<String> jobList;
	private String env;
	
	public ProcessFileThread(){		
	}
		
	
	

	public FileContainer<String> getContainer() {
		return container;
	}

	public void setContainer(FileContainer<String> container) {
		this.container = container;
	}

	public FileContainer<ProcessFile> getProcessFilecontainer() {
		return processFilecontainer;
	}

	public void setProcessFilecontainer(
			FileContainer<ProcessFile> processFilecontainer) {
		this.processFilecontainer = processFilecontainer;
	}

	public List<String> getJobList() {
		return jobList;
	}

	public void setJobList(List<String> jobList) {
		this.jobList = jobList;
	}

	public String getEnv() {
		return env;
	}

	public void setEnv(String env) {
		this.env = env;
	}

	public void run(){

		while(true){

			String fileName=container.getFileFromQueue();
			if(fileName==null)
				break;
			FileBean fb=FileUtil.parseFileName(fileName);
			
			if(fb==null)
				continue;
			
			long jobId=fb.getJobId();
			String jobName=fb.getJobName();
			
			if(jobList.contains(jobId+jobName))
				continue;
		
			ProcessFile processFile= new ProcessFile(fileName,env);
			processFilecontainer.add2Queue(processFile);
			log.info(Thread.currentThread().getName()+"processed "+fileName);
		}
		

			
			
			
		}
}


