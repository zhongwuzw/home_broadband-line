package com.opencassandra.pc_special_data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 用於生成pc专线中间表数据
 * @author kongxiangchun
 *
 */
public class PcMiddleData {

	private static Pattern pattern = Pattern.compile("[,]");
	private static PcMiddleDataDao middleDataDao = new PcMiddleDataDao();

	public static void main(String[] args) {

		
	//	String[] args = {"test_pc_middle_table","pc_ping","test_pc_middle_table","overview_kpi_orgid_bandwidth","201604"};
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i] +"   对应的参数值");
		}
		
		//数据源表所在库名
		String dataSourceBaseName = args[0];
		//数据源表名
		String dataSourceTable = args[1];
		//目标表所在库名
		String targetBaseName = args[2];
		//目标表名
		String targetTable = args[3];
		
		String month = args[4];
		String middels[] = null;
		if(targetTable.indexOf(",")>0){
			 middels = pattern.split(targetTable);
		}else{
			middels = new String[]{targetTable};
		}
		
		for (int i = 0; i < middels.length; i++) {
			String tarTableName = middels[i];

			//查询要入库的表是否存在
			boolean tableFlag = middleDataDao.queryTableExist(tarTableName, targetBaseName);
			
			if(!tableFlag){
				middleDataDao.createTable(tarTableName, targetBaseName);
			
		}
			//查询现月份数据是否存在
			boolean dataFlag = middleDataDao.queryDataExist(tarTableName, targetBaseName, month);
			//存在则删除该月份数据
			if(dataFlag){
				middleDataDao.delMonthData(tarTableName, targetBaseName, month);
			}
			List<Map> listMap = middleDataDao.getData(dataSourceTable, dataSourceBaseName,tarTableName,month,targetBaseName);
			List<String> sqlList = new ArrayList<String>();
			if(listMap.size()>0){
				for (Map map : listMap) {
					String sql = middleDataDao.appendInsertSql(tarTableName,map);
					sqlList.add(sql);
				}
			}
			middleDataDao.dealSqlList(sqlList,targetBaseName);
			listMap.clear();
			sqlList.clear();
		}
		
		// 程序开始时间
		long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		startdate = new Date().getTime();
		System.out.println("开始进行数据读取::::::  " + startdate);
		
		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：： " + enddate + " ::  " + new Date() + "  ::用时：：：  " + (enddate - startdate));
	}
}
