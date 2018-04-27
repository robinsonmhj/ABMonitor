/**
 * 
 */
package com.tmg.quartz.Jobs;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;



import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.tmg.Log.Constant;

import com.tmg.Log.FileContainer;
import com.tmg.Log.ProcessFile;
import com.tmg.Log.ProcessFileThread;
import com.tmg.Util.FileUtil;
import com.tmg.greenplum.DAOImp.GreenplumDAOImp;

/**
 * @author Haojie Ma
 * @date Sep 11, 2015
 */
public class ProdABGPLogMonitorJob extends QuartzJobBean {
	
	
	private static Logger log=Logger.getLogger(ProdABGPLogMonitorJob.class);
	
	@Autowired
	@Qualifier("ProcessFileThread")
	private ProcessFileThread processFileThread;
	
	public void setprocessFileThread(ProcessFileThread processFileThread){
		this.processFileThread=processFileThread;
	}
	
	@Autowired
	@Qualifier("GreenplumDAO")
	private GreenplumDAOImp gpDAOImp;
	
	public void setgpDAOImp(GreenplumDAOImp gpDAOImp){
		this.gpDAOImp=gpDAOImp;
	}
	
	
	protected void executeInternal(JobExecutionContext ctx) throws JobExecutionException {
		
		log.info("Prod ActiveBatch Log Monitor Job Starts");
		
		
		int threadNo=10;
		int days=1;
		try{
			 threadNo=Integer.valueOf(System.getProperty("threadNo"));
			 days=Integer.valueOf(System.getProperty("days"));
		}catch(Exception e){
			log.info("",e);
			
		}
		

		String ext=".log",env="PRD",dir,fileName,date;
		String os=System.getProperty("os.name");
		if(os.toLowerCase().contains("linux"))
			dir=Constant.prd_path_linux;
		else
			dir=Constant.prd_path_windows;
		
		List<String> dateList=new ArrayList<String>();
		List<String> jobList= new ArrayList<String>();
		SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();	

		for(int i=0;i<=days;i++){
			calendar.add(Calendar.DATE,-i);
			Date d1=calendar.getTime();
			date=df.format(d1);
			dateList.add(date);
			List<String> jbList1=gpDAOImp.getJobListByDate(date);
			jobList.addAll(jbList1);
		}
		
		
		List<String> fList=FileUtil.getFileList(dir,ext,dateList);
		Iterator<String> ite=fList.iterator();
		
		FileContainer<String> container= new FileContainer<String>();
		FileContainer<ProcessFile> sqlContainer= new FileContainer<ProcessFile>();
		
		while(ite.hasNext()){
			fileName=ite.next();
			container.add2Queue(fileName);
		}
		
		ProcessFileThread[] processArray= new ProcessFileThread[threadNo];
		Thread[] threadArray= new Thread[threadNo];
		
		for(int i=0;i<threadNo;i++){
			
			processArray[i]=processFileThread;
			processArray[i].setContainer(container);
			processArray[i].setEnv(env);
			processArray[i].setJobList(jobList);
			processArray[i].setProcessFilecontainer(sqlContainer);
			threadArray[i]= new Thread(processFileThread,"Thread"+i);
			threadArray[i].start();			
		}
		
		//let the main thread wait until all the threads below are dead
		for(int i=0;i<threadNo;i++){
			try{
				threadArray[i].join();
			}catch(Exception e){
				log.info("",e);
			}
		}
		
		
		
		List<ProcessFile> list= new ArrayList<ProcessFile>();
		ProcessFile processFile=null;
		while(true){
			
			processFile=sqlContainer.getFileFromQueue();
			if(processFile==null)
				break;
			list.add(processFile);
			
		}
		long start=System.currentTimeMillis();
		gpDAOImp.updateInsertBatch(list);
		long end=System.currentTimeMillis();
		long used=end-start;
		log.info("Insert and Update using:"+used);
		
		
	}
	

}


