/**
 * 
 */
package com.tmg.Action;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.tmg.Log.Constant;
import com.tmg.Log.FileBean;
import com.tmg.Log.ProcessFile;
import com.tmg.Util.FileUtil;
import com.tmg.greenplum.DAOImp.GreenplumDAOImp;





/**
 * @author Haojie Ma
 * @date Jun 5, 2015
 */
public class Main {
	
	
	private static Logger log=Logger.getLogger(Main.class);


	public static void main(String[] args)  {
		
		 //ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		//AbstractApplicationContext context=new ClassPathXmlApplicationContext("applicationContext.xml");;
		//testHibernate();
		//testScheduler();
		//testCallShell();
		
		//Initial Upload
		//List<String> dateList=null;
		//String prd="PRD";
		//testLog(date,prd);
		//String tst="TST";
		//testLog(dateList,tst);

		
		//test();
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		/*
		int length=args.length;
		if(length!=2){
			System.out.println("============================================================================");
			System.out.println("Usage: java -jar xxx.jar threadNumber Days");
			System.out.println("threadNumber is thread number to process files");
			System.out.println("Days is integer, it is indicated the file modify timestamp, \nfor example, if you want to process the files today, enter 0, the files between ysterday and today enter 1");
			System.out.println("============================================================================");
			return;
		}
			
		int threadNo=Integer.valueOf(args[0]);
		int day=Integer.valueOf(args[1]);
		
		System.out.println(threadNo);
		System.out.println(day);
		
		
		
		int threadNo=10;
		int days=1;
		try{
			 threadNo=Integer.valueOf(System.getProperty("threadNo"));
			 days=Integer.valueOf(System.getProperty("days"));
		}catch(Exception e){
			log.info("",e);
			
		}
		
		System.out.println(threadNo+"\n"+days);
		
		*/
		/*
		com.tmg.greenplum.DAOImp.GreenplumDAOImp gpDAOImp = (com.tmg.greenplum.DAOImp.GreenplumDAOImp) context.getBean("GreenplumDAO");
		ProcessFile processFile= new ProcessFile("AMNY_0005192302-09Sep2015-132806_014.log","TST");
		//System.out.println(processFile);
		try{
			gpDAOImp.insert(processFile);
		}catch(Exception e){
			
		}
		*/
		
		log.info("AB Monitor Application Started");
		//System.out.println("Hello");

	}

	
	public static void testCallShell(){
		
		BufferedReader reader=null;
		try{
			Process p = Runtime.getRuntime().exec(new String[]{"bash","-c","netstat -a|grep 'tlisrv'"});
			System.out.println("Hello");
			reader= new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line=reader.readLine();
			String[] lines;
			while(line!=null){
				line=reader.readLine();
				lines=line.split(" ");
				int len=lines.length;
				String tmp=lines[len-2];//host and port
				String host=tmp.split(":")[0];
				System.out.println(host);
			}
		}catch(Exception e){
			
			log.error("",e);
			
		}
		
		
		
		
	}
	
	
	
	
	public static void test(){
		
		
		SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-d");
		Date today= new Date();
		String date=df.format(today);
		System.out.println(date);
		
		
		//testAnnocation personDAO = context.getBean(testAnnocation.class);
		//personDAO.test();
		
		//KillBadQueryJob personDAO = context.getBean(KillBadQueryJob.class);
		//personDAO.execute();
	}
	
	/*
	
	public static void testScheduler() {

		try {

			JobDetail job = JobBuilder.newJob(KillBadQueryJob.class)
					.withIdentity("KillSlowQueryJob").build();

			// Trigger trigger =
			// TriggerBuilder.newTrigger().withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(30).repeatForever()).build();

			CronTrigger cronTrigger = TriggerBuilder
					.newTrigger()
					.withIdentity("KillSlowQueryTrigger", "GemfireXD")
					.withSchedule(
							CronScheduleBuilder.cronSchedule("0/30 * * * * ?"))
					.build();
			//crobtab job format minites,hour,day of month,month,day of week,command linux
			//java seconds,minutes,hours,day of month,month,day of week,year
			SchedulerFactory schFactory = new StdSchedulerFactory();
			Scheduler sch = schFactory.getScheduler();
			sch.start();
			sch.scheduleJob(job, cronTrigger);

		} catch (Exception e) {

			log.error("error",e);

		}

	}
*/

	
	
	public static void testLog(List<String> dateList,String env){
		
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		GreenplumDAOImp gpDAOImp = (com.tmg.greenplum.DAOImp.GreenplumDAOImp) context.getBean("GreenplumDAO");
		
		
		String dir=null;
		String os=System.getProperty("os.name");
		if(env.toUpperCase().equals("PRD")){
			if(os.toLowerCase().contains("linux"))
				dir=Constant.prd_path_linux;
			else
				dir=Constant.prd_path_windows;
			
		}else{
			if(os.toLowerCase().contains("linux"))
				dir=Constant.tst_path_linux;
			else
				dir=Constant.tst_path_windows;
		}
			
		String ext=".log";
		List<String> fList=FileUtil.getFileList(dir,ext,dateList);
		Iterator<String> ite=fList.iterator();
		ProcessFile processFile=null;
		int failedCount=0;
		while(ite.hasNext()){
			String fileName=ite.next();
			FileBean fb=FileUtil.parseFileName(fileName);
			if(fb==null)
				continue;
			long jobId=fb.getJobId();
			String jobName=fb.getJobName();
			if(gpDAOImp.getReportByJobId(jobId,jobName))
				continue;
			processFile= new ProcessFile(fileName,env);
			try{
				//gpDAOImp.insert(processFile);
			}catch(Exception e){
				log.error("The failed bean is "+processFile);
				log.info("",e);
			}
			
			boolean failed=processFile.isfailed();
			if(failed){
				log.info("job failed in "+fileName);
				failedCount++;
			}
			
		}
			
		log.info("Find out "+failedCount+" jobs");
		
		
		/*
		long jobId=5482297;
		String jobName="FileSourceLoad";
		
		List<String> jbList1=gpDAOImp.getJobListByDate("2015-09-14");
		List<String> jbList2=gpDAOImp.getJobListByDate("2015-09-15");
		
		
		System.out.println(jbList1.size());
		System.out.println(jbList2.size());
		
		List<String> jobList= new ArrayList<String>();
		jobList.addAll(jbList1);
		jobList.addAll(jbList2);
		
		System.out.println(jobList.size());
		
		
		Iterator<String> iterator=jobList.iterator();
		while(iterator.hasNext())
			System.out.println(iterator.next());
		
		
		if(jobList.contains(jobId+jobName))
			System.out.println("I find you");
			*/
		
	}
	
	


}
