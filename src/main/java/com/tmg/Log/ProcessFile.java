/**
 * 
 */
package com.tmg.Log;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.tmg.Util.FileUtil;


/**
 * @author Haojie Ma
 * @date Sep 8, 2015
 */
public class ProcessFile {
	
	
	private static Logger log=Logger.getLogger(ProcessFile.class);
	
	private boolean failed;
	private String message;
	private Date JobStartTime;
	private Date JobCompletedTime;
	private String fileName;
	private FileBean fileBean;
	private String env;
	

	public FileBean getFileBean() {
		return fileBean;
	}



	public void setFileBean(FileBean fileBean) {
		this.fileBean = fileBean;
	}


	public Date getJobStartTime() {
		return JobStartTime;
	}



	public void setJobStartTime(Date jobStartTime) {
		JobStartTime = jobStartTime;
	}



	public Date getJobCompletedTime() {
		return JobCompletedTime;
	}



	public void setJobCompletedTime(Date jobCompletedTime) {
		JobCompletedTime = jobCompletedTime;
	}



	public boolean isfailed() {
		return failed;
	}



	public void setfailed(boolean failed) {
		this.failed = failed;
	}



	public String getMessage() {
		return message;
	}



	public void setMessage(String message) {
		this.message = message;
	}

	

	public String getEnv() {
		return env;
	}



	public void setEnv(String env) {
		this.env = env;
	}

	
	public ProcessFile(){
		
	}


	public ProcessFile(String fileName,String env){
		this.fileName=fileName;
		this.env=env;
		begin();
	}
	
	
	
	private void begin(){
		StringBuilder sb=new StringBuilder();
		fileBean=FileUtil.parseFileName(fileName);
		String path=null;
		if(env==null||env.trim().equals(""))
			return;
		env=env.toUpperCase();
		String os=System.getProperty("os.name");
		if(env.toUpperCase().equals("PRD")){
			if(os.toLowerCase().contains("linux"))
				path=Constant.prd_path_linux;
			else
				path=Constant.prd_path_windows;
			
		}else{
			if(os.toLowerCase().contains("linux"))
				path=Constant.tst_path_linux;
			else
				path=Constant.tst_path_windows;
		}
		
		BufferedReader reader=null;
		Matcher matcher;
		try{
			
			reader= new BufferedReader(new FileReader(new File(path+fileName)));
			String line=reader.readLine();
			String timeStampReg="(NOTICE:\\s{1,}||^)\\d{2,}[:-]\\d{2}[:-]\\d{2}.*";
			Pattern pattern=Pattern.compile(timeStampReg);
			boolean flag=false;//flag indicating first time to match time stamp
			int count=0;
			while(line!=null){
				if(line.trim().equals("")){
					line=reader.readLine();
					continue;
				}
				//only append 5 lines after finding the error
				if(line.toLowerCase().contains("error")||line.toLowerCase().contains("failed")){
					failed=true;
					//the following 2 lines is used when there is no timestamp in the whole log file
					sb.append(line);
					flag=true;
				}else if(line.startsWith(Constant.jobStartTime)){
					String[] lines=line.split("\\s{1,}:");
					JobStartTime=String2Date(lines[1].trim());
				}else if(line.startsWith(Constant.jobCompleteTime)){
					String[] lines=line.split("\\s{1,}:");
					JobCompletedTime=String2Date(lines[1].trim());
				}
				//System.out.println(line);	
				matcher = pattern.matcher(line);
				if(matcher.matches()){
					//System.out.println(line);	
					if(sb.length()!=0&&!failed)
						sb.delete(0, sb.length());
					flag=true;
				}
				if(flag){
					if(failed)
						count++;
					if(count<=Constant.maxLineNo){
						sb.append(line);
						sb.append("\n");
					}

				}
				line=reader.readLine();
			}
			
			if(!failed)
				message=null;
			else
				message=sb.toString();
			
			
		}catch(Exception e){
			
			log.info("",e);
			
		}finally{
			
			try{
				reader.close();
				
			}catch(Exception e){
				
				log.error("Close file error",e);
				
			}	
		}
	}
	
	
	private Date String2Date(String d){
		try{
			DateFormat df = new SimpleDateFormat("M/dd/yyyy h:mm:ss a");
			Date date=df.parse(d);
			return date;
		}catch(Exception e){
			
			log.info("",e);
			
		}

		return null;
		
		
		
	}
	
	
	
	
	@Override
	public String toString() {
		return "ProcessFile [failed=" + failed + ", message=" + message
				+ ", JobStartTime=" + JobStartTime + ", JobCompletedTime="
				+ JobCompletedTime + ", fileName=" + fileName + ", fileBean="
				+ fileBean + ", env=" + env + "]";
	}



	public static void main(String[] args){
		
		//ProcessFile file= new ProcessFile("File_Type_Validate_0005055707-06Sep2015-185807_014.log","TST");
		ProcessFile file= new ProcessFile("AMNY_0005192302-09Sep2015-132806_014.log","TST");
		
		/*
		System.out.println(file.isfailed());
		System.out.println(file.getMessage());
		
		String value="0000102340";
		long a=Long.valueOf(value);
		System.out.println(a);
		
		String s="NOTICE:  2015-09-09 : ods.fn_fac_dbo_cmc_meme_member_ms('AMNY', '<NULL>') for source table s_fac_dbo_cmc_meme_member_ms";
		//String s="2015-09-09 08:28:59.214846-05";
		Pattern pattern=Pattern.compile("(NOTICE:\\s{1,}||^)\\d{2,}[:-]\\d{2}[:-]\\d{2}.*");
		if(pattern.matcher(s).matches()){
			System.out.println("true");
			System.out.println(s);
		}
		*/
		Date date=file.String2Date("9/10/2015 7:00:12 AM");
		java.sql.Timestamp d= new java.sql.Timestamp(date.getTime());
		System.out.println(d);
				
		
	}



	
	
	
	

}


