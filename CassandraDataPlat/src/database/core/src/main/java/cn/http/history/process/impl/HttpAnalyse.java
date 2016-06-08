package cn.http.history.process.impl;

import java.util.List;
import java.util.Map;

import cn.http.history.dao.HttpInsertDao;
import cn.speed.history.dao.ResultUploadDao;
import cn.speed.history.process.ResultFileAnalyse;
import cn.speed.history.process.ResultFileDataBase;

public class HttpAnalyse extends ResultFileDataBase implements ResultFileAnalyse{

	public HttpAnalyse(){
		this.setSustained(true);
	}
	
	public HttpAnalyse(List<Map<String,Object>> srcResultSet, String tableName, String databaseName){
		this.setSrcResultSet(srcResultSet);
		this.setTableName(tableName);
		this.setDatabaseName(databaseName);
	}
	
	@Override
	public List<Map<String,Object>> anlyse() {
		// TODO Auto-generated method stub
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
		HttpInsertDao myHID = new HttpInsertDao();
		DealWithHttpAbs myDWSA = new DealWithHttpAbs();
		
		List<Map<String,Object>> resultList = myRUD.findByType("http", new int[]{0,3});
		
		if(resultList!=null && resultList.size()!=0){
			for(int i=0; i<resultList.size(); i++){
				try {
					boolean isSuccess = myDWSA.findFile(resultList.get(i));
					myDWSA.getResultSet().put("org_id", resultList.get(i).get("org_id"));
					if(isSuccess){
						myHID.insert(myDWSA.getResultSet());
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
		if(this.isSustained()){
			while(true){
				this.deal();
				try {
					Thread.sleep(60000);//60000
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}else{
			this.anlyse();
		}
	}
	
	public static void main(String[] args){
		HttpAnalyse my = new HttpAnalyse();
		my.deal();
	}

	@Override
	public boolean dealWithSpec() {
		// TODO Auto-generated method stub
		ResultUploadDao myRUD = new ResultUploadDao();
		HttpInsertDao myHID = new HttpInsertDao();
		DealWithHttpAbs myDWSA = new DealWithHttpAbs();
		List<Map<String,Object>> srcResultList = this.getSrcResultSet();
		for(int i=0; i<srcResultList.size(); i++){
			List<Map<String,Object>> resultList = myRUD.findByStorePath("http", (String)srcResultList.get(i).get("store_path"));
			for(int j=0; j<resultList.size(); j++){
				try {
					int times = 1;
					boolean isSuccess = false;
					while(!isSuccess&&times<=2){
						isSuccess = myDWSA.findFile(resultList.get(j));
						if(!isSuccess){
							Thread.sleep(5000);
							times++;
						}
					}
					if(isSuccess){
						myDWSA.getResultSet().put("org_id", resultList.get(j).get("org_id"));
						myHID.insert(myDWSA.getResultSet());
						myRUD.updateById((Integer)(resultList.get(j).get("id")),"1");
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					myRUD.updateById((Integer)(resultList.get(j).get("id")),"4");
					continue;
				}
			}
		}
		return false;
	}
}
