/**
 * 
 */
package com.tmg.Util;

import java.io.File;
import java.io.FilenameFilter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;



import org.apache.log4j.Logger;

import com.tmg.Log.Constant;
import com.tmg.Log.FileBean;

/**
 * @author Haojie Ma
 * @date Sep 9, 2015
 */
public class FileUtil {
	
	
	
	private static Logger log=Logger.getLogger(FileUtil.class);
	
	//the format for the ext should be ".ext", ".log"
	//the format for the date should be "YYYY-MM-DD"
	public static List<String> getFileList(String directory,String ext,List<String> dateList){
		
		List<String> fileList= new ArrayList<String>();
		
		File dir= new File(directory);
		if(!dir.isDirectory())
			return null;
		final String fileExt=ext;
		
		FilenameFilter logFilter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				String lowercaseName = name.toLowerCase();
				if (lowercaseName.endsWith(fileExt)) {
					return true;
				} else {
					return false;
				}
			}
		};
		long start=System.currentTimeMillis();
		//String[] fileArray=dir.list(); it takes 219 seconds to list all the files
		String[] fileArray= dir.list(logFilter);// it takes 196 seconds to list all the files
		long end=System.currentTimeMillis();
		long using=end-start;
		log.info("List file using "+using);
		for(int i=0;i<fileArray.length;i++){
			String file=fileArray[i];
			if(dateList==null)
				fileList.add(file);
			else{
				FileBean f=parseFileName(file);
				if(f!=null&&dateList.contains(f.getDate()))
					fileList.add(file);
			}	
		}
		
		return fileList;
		
		
		
		
	}
	

	public static String convertMill2Date(long millecond){
		long start=System.currentTimeMillis();
		StringBuilder date=new StringBuilder();
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(millecond);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH)+1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		

		String delimiter="-";
		date.append(year).append(delimiter);
		if(month<10)
			date.append(0).append(month);
		else
			date.append(month);
		
		date.append(delimiter);
		
		
		if(day<10)
			date.append(0).append(day);
		else
			date.append(day);
		
		long end=System.currentTimeMillis();
		long using=end-start;
		if(using>10)
			log.info("convert date using time"+using);
		return date.toString();
		
		
	}

	
	private static String firstCap(String str){
		
		if(str==null||str.trim().equals(""))
			return null;
		String f=str.toUpperCase().substring(0,1);
		String left=str.toLowerCase().substring(1,str.length());
		return f+left;
		
		
	}
	
	public synchronized static FileBean parseFileName(String fileName){
		
		
		String[] array=fileName.split("-");
		if(array.length!=3){
			log.error(fileName+" invalid!");
			return null;
		}
			
		String[] job=array[0].split("_");
		int len=job.length;
		String jobName="";
		for(int i=0;i<len-1;i++)
			jobName+=firstCap(job[i]);
		long jobId=Long.valueOf(job[len-1]);
		String date=array[1];
		date=formatDate(date);
		//System.out.println(date);
		
		FileBean bean= new FileBean(jobName,jobId,date);
		return bean;
		
		
		
		
	}
	
	private static String formatDate(String date){
		
		try{
			DateFormat df = new SimpleDateFormat("ddMMMyyyy");
			StringBuilder dateString= new StringBuilder();
			Date d=df.parse(date);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(d);
			int year = calendar.get(Calendar.YEAR);
			int month = calendar.get(Calendar.MONTH)+1;// Jan is 0,Feb 1, plus 1 to the month
			int day = calendar.get(Calendar.DAY_OF_MONTH);
			
			String delimiter="-";
			dateString.append(year).append(delimiter);
			if(month<10)
				dateString.append(0).append(month);
			else
				dateString.append(month);
			
			dateString.append(delimiter);
			
			
			if(day<10)
				dateString.append(0).append(day);
			else
				dateString.append(day);
			
			return dateString.toString();
			
		}catch(Exception e){
			log.error("",e);
		}
		
		return null;
		
	}
	
	public static void main(String[] args){
		
		/*
		//String dir="C:\\work\\SVN\\ODS\\GF_ODS\\Standard_Version\\Dev_Sprint\\03_Source_Code\\01_Database\\01_DDL\\Gemfire";
		String dir=Constant.prd_path_windows;
		String ext=".log";
		String date="2015-09-10";
		List<String> fList=getFileList(dir,ext,date);
		
		
		Iterator<String> ite=fList.iterator();
		while(ite.hasNext())
			System.out.println(ite.next());
		*/
		
		
		System.out.println(convertMill2Date(1441846952648l));
		
		/*SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE,-1);
		Date yesterday=calendar.getTime();
		String date=df.format(yesterday);
		System.out.println(date);*/
		
		//System.out.println(formatDate("08Jan2015"));

		
		
	}
	
	
	
	
	

}


