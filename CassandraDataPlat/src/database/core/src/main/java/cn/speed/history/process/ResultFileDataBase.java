package cn.speed.history.process;

import java.util.List;
import java.util.Map;

public abstract class ResultFileDataBase extends Thread {
	String filePath;
	String testType;
	String tableName;
	String databaseName;
	boolean isInserted = false;
	boolean isTransfered = false;
	boolean isSustained = false;
	List<Map<String,Object>> srcResultSet;
	
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public String getTestType() {
		return testType;
	}
	public void setTestType(String testType) {
		this.testType = testType;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public boolean isInserted() {
		return isInserted;
	}
	public void setInserted(boolean isInserted) {
		this.isInserted = isInserted;
	}
	public boolean isTransfered() {
		return isTransfered;
	}
	public void setTransfered(boolean isTransfered) {
		this.isTransfered = isTransfered;
	}
	public List<Map<String,Object>> getSrcResultSet() {
		return srcResultSet;
	}
	public void setSrcResultSet(List<Map<String,Object>> srcResultSet) {
		this.srcResultSet = srcResultSet;
	}
	public boolean isSustained() {
		return isSustained;
	}
	public void setSustained(boolean isSustained) {
		this.isSustained = isSustained;
	}
}
