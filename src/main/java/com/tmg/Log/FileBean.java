/**
 * 
 */
package com.tmg.Log;

/**
 * @author Haojie Ma
 * @date Sep 9, 2015
 */
public class FileBean {
	
	private String date;
	private String jobName;
	private long jobId;
	
	public FileBean(String jobName,long jobId,String date){
		
		this.date=date;
		this.jobName=jobName;
		this.jobId=jobId;
		
		
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public long getJobId() {
		return jobId;
	}

	public void setJobId(long jobId) {
		this.jobId = jobId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + (int) (jobId ^ (jobId >>> 32));
		result = prime * result + ((jobName == null) ? 0 : jobName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FileBean other = (FileBean) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (jobId != other.jobId)
			return false;
		if (jobName == null) {
			if (other.jobName != null)
				return false;
		} else if (!jobName.equals(other.jobName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FileBean [date=" + date + ", jobName=" + jobName + ", jobId="
				+ jobId + "]";
	}
	
	
	
	
	
	
	
	
	

}


