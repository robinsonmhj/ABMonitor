/**
 * 
 */
package com.tmg.greenplum.DAOImp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.tmg.Log.ProcessFile;
import com.tmg.greenplum.DAO.CommonDAO;


/**
 * @author Haojie Ma
 * @date Sep 10, 2015
 */
public class GreenplumDAOImp implements CommonDAO{
	
	private static Logger log= Logger.getLogger(GreenplumDAOImp.class);
	
	@Autowired
	@Qualifier("dataSourceGreenplum")
	private DataSource dataSource;
	
	
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	
	
	
	public boolean getReportByJobId(long jobId,String jobName){
		
		ResultSet rs = null;
		Connection conn=null;
		String sql="select count(1) as count from odm.ab_report where job_id=? and job_name=?";
		try{
			conn=dataSource.getConnection();
			PreparedStatement ps=conn.prepareStatement(sql);
			ps.setLong(1, jobId);
			ps.setString(2, jobName);
			rs=ps.executeQuery();
			int count=0;
			while (rs.next()) {
				count=rs.getInt("count");
			}
			
			if(count!=0)
				return true;
			
		}catch(Exception e){
			
			log.info("Exception:",e);
			
		}finally{
			
			try{
				if(rs!=null)
					rs.close();
				conn.close();
			}catch(Exception e){
				log.error("Close connection error",e);
			}
			
		}
		
		return false;
		
	}
	
public void updateBatch(List<ProcessFile> fileList){
		
		if(fileList==null)
			return;
	
		Connection conn=null;
		PreparedStatement ps=null;
		String sql="update odm.ab_report set start_time=?,complete_time=?,message=?,failed=? where job_id=? and job_name=?";
		Iterator<ProcessFile> iterator= fileList.iterator();
		try{
			conn=dataSource.getConnection();
			ps=conn.prepareStatement(sql);
			while(iterator.hasNext()){
				ProcessFile file=iterator.next();
				long jobId=file.getFileBean().getJobId();
				String jobName=file.getFileBean().getJobName();
				Timestamp startTime=null,completeTime=null;
				
				if(file.getJobStartTime()!=null)
					startTime=new Timestamp(file.getJobStartTime().getTime());
				if(file.getJobCompletedTime()!=null)
					completeTime=new Timestamp(file.getJobCompletedTime().getTime());
				boolean status=file.isfailed();
				String message=file.getMessage();
				if(message!=null)
					message=message.replaceAll("'", "'").replace("\\", "\\\\");

				ps.setTimestamp(1, startTime);
				ps.setTimestamp(2, completeTime);
				ps.setString(3, message);
				ps.setBoolean(4, status);
				ps.setLong(5, jobId);
				ps.setString(6, jobName);
			}
			
			ps.executeBatch();
			
			
		}catch(Exception e){
			log.info("",e);
		}finally{
			
			try{
				if(ps!=null)
					ps.close();
				if(conn!=null)
					conn.close();
			}catch(Exception e){
				log.error("Close connection error",e);
			}
	
			
		}
		
	
		
	}
	
	
	
	
	public void insertBatch(List<ProcessFile> fileList){
		
		//int insertCount=-1;
		if(fileList==null)
			return ;
		Iterator<ProcessFile> iterator=fileList.iterator();
		Connection conn=null;
		PreparedStatement ps=null;
		//jobId,jobName,startTime,completeTime,failed,message,env
		String sql="insert into  odm.ab_report values(?,?,?,?,?,?,?)";
		try{
			
			conn=dataSource.getConnection();
			ps=conn.prepareStatement(sql);
			
			while(iterator.hasNext()){
				ProcessFile file=iterator.next();
				long jobId=file.getFileBean().getJobId();
				String jobName=file.getFileBean().getJobName();
				Timestamp startTime=null,completeTime=null;
				
				if(file.getJobStartTime()!=null)
					startTime=new Timestamp(file.getJobStartTime().getTime());
				if(file.getJobCompletedTime()!=null)
					completeTime=new Timestamp(file.getJobCompletedTime().getTime());
				boolean status=file.isfailed();
				String message=file.getMessage();
				if(message!=null)
					message=message.replaceAll("'", "'").replace("\\", "\\\\");
				String env=file.getEnv();
				ps.setLong(1, jobId);
				ps.setString(2, jobName);
				ps.setTimestamp(3, startTime);
				ps.setTimestamp(4, completeTime);
				ps.setBoolean(5, status);
				ps.setString(6, message);
				ps.setString(7, env);
				ps.addBatch();
				
				
			}
			
			ps.executeBatch();
			
			
		}catch(Exception e){
			
			log.info("",e);
			
		}finally{
			
			try{
				if(ps!=null)
					ps.close();
				if(conn!=null)
					conn.close();
			}catch(Exception e){
				log.error("close connection error",e);
			}
			
			
		}
		
	}
	
	public void updateInsertBatch(List<ProcessFile> fileList){
		
		if(fileList==null)
			return ;
		Iterator<ProcessFile> iterator=fileList.iterator();
		Connection conn=null;
		PreparedStatement updatePs=null;
		PreparedStatement insertPs=null;
		//jobId,jobName,startTime,completeTime,failed,message,env
		String updateSql="update odm.ab_report set start_time=?,complete_time=?,message=?,failed=? where job_id=? and job_name=?";
		String insertSql="insert into  odm.ab_report  select ?,?,?,?,?,?,? where not exists (select 1 from odm.ab_report where job_id=? and job_name=? )";
		try{
			
			conn=dataSource.getConnection();
			updatePs=conn.prepareStatement(updateSql);
			insertPs=conn.prepareStatement(insertSql);
			
			while(iterator.hasNext()){
				ProcessFile file=iterator.next();
				long jobId=file.getFileBean().getJobId();
				String jobName=file.getFileBean().getJobName();
				Timestamp startTime=null,completeTime=null;
				
				if(file.getJobStartTime()!=null)
					startTime=new Timestamp(file.getJobStartTime().getTime());
				if(file.getJobCompletedTime()!=null)
					completeTime=new Timestamp(file.getJobCompletedTime().getTime());
				boolean status=file.isfailed();
				String message=file.getMessage();
				if(message!=null)
					message=message.replaceAll("'", "'").replace("\\", "\\\\");
				String env=file.getEnv();
				
				updatePs.setTimestamp(1, startTime);
				updatePs.setTimestamp(2, completeTime);
				updatePs.setString(3, message);
				updatePs.setBoolean(4, status);
				updatePs.setLong(5, jobId);
				updatePs.setString(6, jobName);
				updatePs.addBatch();
				
				
				insertPs.setLong(1, jobId);
				insertPs.setString(2, jobName);
				insertPs.setTimestamp(3, startTime);
				insertPs.setTimestamp(4, completeTime);
				insertPs.setBoolean(5, status);
				insertPs.setString(6, message);
				insertPs.setString(7, env);
				insertPs.setLong(8, jobId);
				insertPs.setString(9, jobName);
				insertPs.addBatch();
				
				
			}
			
			updatePs.executeBatch();
			insertPs.executeBatch();
			
		}catch(SQLException e){
			
			while(e.getNextException()!=null){
				Exception ex=e.getNextException();
				log.info("",ex);
			}
			
			
		}finally{
			
			try{
				if(updatePs!=null)
					updatePs.close();
				if(insertPs!=null)
					insertPs.close();
				if(conn!=null)
					conn.close();
			}catch(Exception e){
				log.error("close connection error",e);
			}
			
			
		}
		
		
		
	}
	
	
	
	//the format of the date should be 'YYYY-MM-DD'
	public List<String> getJobListByDate(String date){
		List<String> jobList= new ArrayList<String>();
		
		ResultSet rs = null;
		Connection conn=null;
		String sql="select job_id,job_name from odm.ab_report where to_char(start_time,'YYYY-MM-DD')=?";
		long job_id;
		String job_name;
		StringBuilder mix;
		try{
			conn=dataSource.getConnection();
			PreparedStatement ps=conn.prepareStatement(sql);
			ps.setString(1, date);
			rs=ps.executeQuery();
			while (rs.next()) {
				mix=new StringBuilder();
				job_id=rs.getLong("job_id");
				job_name=rs.getString("job_name");
				mix.append(job_id);
				mix.append(job_name);
				jobList.add(mix.toString());
			}
		}catch(Exception e){
			
			log.info("Exception:",e);
			
		}finally{
			
			try{
				if(rs!=null)
					rs.close();
				if(conn!=null)
					conn.close();
			}catch(Exception e){
				log.error("Close connection error",e);
			}
			
		}
		
		
		return jobList;
		
		
		
	}
	
	
	

}


