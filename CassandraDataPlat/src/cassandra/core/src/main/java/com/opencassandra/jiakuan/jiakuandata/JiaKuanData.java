package com.opencassandra.jiakuan.jiakuandata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.service.utils.UtilDate;

/**
 * 把中间表数据重新计算生成转移到对应的统计表中
 * 
 * @author kongxiangchun
 * 
 */
public class JiaKuanData {

	private static Pattern pattern = Pattern.compile("[,]");
	private static Pattern pattern1 = Pattern.compile("[-]");
	private static JiaKuanDao jiaKuanDao = new JiaKuanDao();

	public static void main(String[] args) {

		// 程序开始时间
		long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		startdate = new Date().getTime();
		System.out.println("开始进行数据读取::::::  " + startdate);

		
		 /* String[] args = { "test_statistic_jiakuan",
		 "overview_kpi_orgid_operator-network_operator,overview_kpi_orgid_terminal-terminal_model,overview_kpi_orgid_terminal-terminal_os"
		  , "test_statistic_jiakuan", "ios,android", "201605" };*/
		 

		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i] + "   对应的参数值");
		}

		// 数据源表所在库名
		String dataSourceBaseName = args[0];
		// 数据源表名|目标表名
		String dataSourceTable = args[1];
		// 目标表所在库名
		String targetBaseName = args[2];

		String parameters = args[3];
		// 月份时间
		String month = args[4];
		String dataSource[] = null;
		if (dataSourceTable.indexOf(",") > 0) {
			dataSource = pattern.split(dataSourceTable);
		} else {
			dataSource = new String[] { dataSourceTable };
		}

		String arguments[] = null;

		if (parameters.indexOf(",") > 0) {
			arguments = pattern.split(parameters);
		} else {
			arguments = new String[] { parameters };
		}

		for (int i = 0; i < dataSource.length; i++) {
			String source = dataSource[i];
			if (source.indexOf("-") > 0) {
				String tarandsour[] = pattern1.split(source);
				String tartable = tarandsour[1];
				for (int j = 0; j < arguments.length; j++) {
					String argument = arguments[j];

					// 数据源表名
					String middel = tarandsour[0] + "_" + argument;
					// 结果表名

					// 查询要入库的表是否存在

					/*
					 * boolean tableFlag = jiaKuanDao.queryTableExist(tartable,
					 * targetBaseName); if (!tableFlag) {
					 * jiaKuanDao.createTable(tartable, targetBaseName); }
					 */

					// 查询现月份数据是否存在
					boolean dataFlag = jiaKuanDao.queryDataExist(tartable, targetBaseName, argument, month);
					// 存在则删除该月份数据
					if (dataFlag) {
						jiaKuanDao.delMonthData(tartable, targetBaseName, argument, month);
					}

					List<Map> listMap = new ArrayList<Map>();
					if (middel.contains("bandwidth") && tartable.equals("overview_kpi")) {
						// overview_kpi表数据
						listMap = jiaKuanDao.getDataKpi(middel, dataSourceBaseName, tartable, month);
					} else if (middel.contains("bandwidth") && tartable.equals("overview_province")) {
						// overview_province表数据
						listMap = jiaKuanDao.getDataProvince(middel, dataSourceBaseName, tartable, month);
					} else if (middel.contains("operator") && tartable.equals("network_operator")) {
						// network_operator 表数据
						listMap = jiaKuanDao.getDataOperator(middel, dataSourceBaseName, tartable, month);
					} else if (middel.contains("operator") && tartable.equals("network_type")) {
						// network_type 表数据
						listMap = jiaKuanDao.getDataNetType(middel, dataSourceBaseName, tartable, month);
					} else if (middel.contains("terminal") && tartable.equals("terminal_os")) {
						// terminal_os 表数据
						listMap = jiaKuanDao.getDataTerminalOs(middel, dataSourceBaseName, tartable, month);
					} else if (middel.contains("terminal") && tartable.equals("terminal_model")) {
						// terminal_model 表数据
						listMap = jiaKuanDao.getDataTerminalModel(middel, dataSourceBaseName, tartable, month);
					}
					List<String> sqlList = new ArrayList<String>();
					if (listMap.size() > 0) {
						for (Map map : listMap) {
							String sql = jiaKuanDao.appendInsertSql(tartable, map);
							sqlList.add(sql);
						}
					}
					jiaKuanDao.dealSqlList(sqlList, targetBaseName);
					listMap.clear();
					sqlList.clear();

					if ( tartable.contains("overview_kpi")) {
						
						String lastmonth = UtilDate.getPreviousMonth(month);
						jiaKuanDao.queryLastMonthDataExist(tartable, targetBaseName, lastmonth, month,argument);
					}
				}
			}

		}
		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：： " + enddate + " ::  " + new Date() + "  ::用时：：：  " + (enddate - startdate));
	}
}