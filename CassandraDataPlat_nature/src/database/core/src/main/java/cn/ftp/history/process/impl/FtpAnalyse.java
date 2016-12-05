package cn.ftp.history.process.impl;

import java.util.List;
import java.util.Map;

import cn.ftp.history.dao.FtpInsertDao;
import cn.speed.history.dao.ResultUploadDao;
import cn.speed.history.process.ResultFileAnalyse;
import cn.speed.history.process.ResultFileDataBase;

public class FtpAnalyse extends ResultFileDataBase implements ResultFileAnalyse{

	public FtpAnalyse(){
		
	}
	
	public FtpAnalyse(List<Map<String,Object>> srcResultSet, String tableName, String databaseName){
		this.setSrcResultSet(srcResultSet);
		this.setTableName(tableName);
		this.setDatabaseName(databaseName);
		
	}
	
	@Override
	public List<Map<String,Object>> anlyse() {
		// TODO Auto-generated method stub
		List<Map<String,Object>> mySrcResultSet = this.getSrcResultSet();
		
		return null;
	}

	@Override
	public boolean insert2Database(List<String> data) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean clearLog(String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@SuppressWarnings("finally")
	@Override
	public boolean deal() {
		// TODO Auto-generated method stub
		ResultUploadDao myRUD = new ResultUploadDao();
		FtpInsertDao myFID = new FtpInsertDao();
		DealWithFtpAbs myDWSA = new DealWithFtpAbs();
		
		List<Map<String,Object>> resultList = myRUD.findByType("ftp",new int[]{0,3});
		
		if(resultList!=null && resultList.size()!=0){
			for(int i=0; i<resultList.size(); i++){
				try {
					boolean isSuccess = myDWSA.findFile(resultList.get(i));
					myDWSA.getResultSet().put("org_id", resultList.get(i).get("org_id"));
					if(isSuccess){
						myFID.insert(myDWSA.getResultSet());
						myRUD.updateById((Integer)(resultList.get(i).get("id")),"1");
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					myRUD.updateById((Integer)(resultList.get(i).get("id")),"4");
					continue;
				}
			}
		}
		return false;
	}

	public void run() {
		while(true){
			this.deal();
			try {
				Thread.sleep(60000);//60000
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args){
		FtpAnalyse my = new FtpAnalyse();
		my.deal();
	}

	@Override
	public boolean dealWithSpec() {
		// TODO Auto-generated method stub
		return false;
	}
}
