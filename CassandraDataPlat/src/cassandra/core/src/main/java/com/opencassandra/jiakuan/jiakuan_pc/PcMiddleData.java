package com.opencassandra.jiakuan.jiakuan_pc;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 用於生成pc专线中间表数据
 * 
 * @author kongxiangchun
 * 
 */
public class PcMiddleData {

	private static Pattern pattern = Pattern.compile("[,]");
	private static PcMiddleDataDao middleDataDao = new PcMiddleDataDao();

	public static void main(String[] args) {

		/*String[] args = { "test_appreportdata_jiakuan", "pc_web_browsing,pc_http_test", "test_middle_table_jiakuan", "overview_kpi_orgid_bandwidth,overview_kpi_orgid_operator,overview_kpi_orgid_terminal" , "auth", "pc",
				"201605" };*/
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i] + "   对应的参数值");
		}

		// 数据源表所在库名
		String dataSourceBaseName = args[0];
		// 数据源表名
		String dataSourceTable = args[1];
		// 目标表所在库名
		String targetBaseName = args[2];
		// 目标表名
		String targetTable = args[3];

		String constantBaseName=args[4];
		
		String parameters = args[5];

		String month = args[6];
		String middels[] = null;
		if (targetTable.indexOf(",") > 0) {
			middels = pattern.split(targetTable);
		} else {
			middels = new String[] { targetTable };
		}
		
		
		String sourceTables[] = null;

		if (dataSourceTable.indexOf(",") > 0) {
			sourceTables = pattern.split(dataSourceTable);
		} else {
			sourceTables = new String[] { dataSourceTable };
		}

		String arguments[] = null;
		if (parameters.indexOf(",") > 0) {
			arguments = pattern.split(parameters);
		} else {
			arguments = new String[] { parameters };
		}

		
		middleDataDao.createTempTable(dataSourceBaseName, targetBaseName, constantBaseName, sourceTables,month);
		
		
		// 获取到sh脚本中配置的探针类型
		for (int j = 0; j < arguments.length; j++) {

			String argument = arguments[j];

			for (int i = 0; i < middels.length; i++) {
				String tarTableName = middels[i] + "_" + argument;

				// 查询要入库的表是否存在
				boolean tableFlag = middleDataDao.queryTableExist(tarTableName, targetBaseName);

				if (!tableFlag) {
					middleDataDao.createTable(tarTableName, targetBaseName);

				}
				// 查询现月份数据是否存在
				boolean dataFlag = middleDataDao.queryDataExist(tarTableName, targetBaseName, month);
				// 存在则删除该月份数据
				if (dataFlag) {
					middleDataDao.delMonthData(tarTableName, targetBaseName, month);
				}
				List<Map> listMap = middleDataDao.getData("jiakuan_pc_temp", dataSourceBaseName, tarTableName, month, targetBaseName, argument,constantBaseName);
				List<String> sqlList = new ArrayList<String>();
				if (listMap.size() > 0) {
					for (Map map : listMap) {
						String sql = middleDataDao.appendInsertSql(tarTableName, map);
						sqlList.add(sql);
					}
				}
				middleDataDao.dealSqlList(sqlList, targetBaseName);
				listMap.clear();
				sqlList.clear();
			}
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
