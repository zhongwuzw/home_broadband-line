package com.opencassandra.descfile;

import java.sql.SQLException;
import java.util.Date;

import com.opencassandra.v01.dao.impl.UpdateGPSDateDao;

public class UpdateGPSDate {

	public static String table[] = ConfParser.table;
	public UpdateGPSDateDao updateDao = new UpdateGPSDateDao();
	
	public static void main(String[] args) {
		UpdateGPSDate updateGPSDate = new UpdateGPSDate();
		updateGPSDate.updateTable();
	}
	public  void updateTable(){
		//http_201509_1,speed_test_201509_1,ping_201509_1,http_201510_1,ping_201510_1,speed_test_201510_1table
		if(table!=null && table.length>0 && table[0]!=null){
			for (int i = 0; i < table.length; i++) {
				String tableName = table[i];
				updateDao.updateGPS(tableName,0,999);
			}	
		}else{
			Date date = new Date();
			int year = date.getYear()+1900;
			int month = date.getMonth()+1;
			if(month<10)
			{
				table = updateDao.getAllTable(year+"0"+month);	
			}else{
				table = updateDao.getAllTable(year+""+month);
			}
			for (int i = 0; i < table.length; i++) {
				String tableName = table[i];
				System.out.println(tableName+" 正在处理");
				updateDao.updateGPS(tableName,0,1000);
				
			}
		}
	}
}
