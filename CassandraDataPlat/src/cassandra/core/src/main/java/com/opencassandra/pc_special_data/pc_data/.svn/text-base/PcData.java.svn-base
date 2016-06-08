package com.opencassandra.pc_special_data.pc_data;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 把中间表数据重新计算生成转移到对应的专线统计表中
 * 
 * @author kongxiangchun
 * 
 */
public class PcData {

	private static Pattern pattern = Pattern.compile("[,]");
	private static Pattern pattern1 = Pattern.compile("[-]");
	private static PcDao pcDao = new PcDao();

	public static void main(String[] args) {

		// 程序开始时间
		long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		startdate = new Date().getTime();
		System.out.println("开始进行数据读取::::::  " + startdate);

		//String[] args = { "map_data_statistic", "overview_kpi_orgid_bandwidth-overview_province", "map_data_statistic", "201604" };
		/*String[] args = { "pc_middle_table", "overview_kpi_orgid_bandwidth|overview_kpi,overview_kpi_orgid_bandwidth|overview_province,overview_kpi_orgid_operator|network_type,overview_kpi_orgid_operator|network_operator,overview_kpi_orgid_terminal|terminal_model,overview_kpi_orgid_terminal|terminal_os", 
							"map_data_statistic", "201604" };*/
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i] + "   对应的参数值");
		}

		// 数据源表所在库名
		String dataSourceBaseName = args[0];
		// 数据源表名|目标表名
		String dataSourceTable = args[1];
		// 目标表所在库名
		String targetBaseName = args[2];
		//月份时间
		String month = args[3];
		String dataSource[] = null;
		if (dataSourceTable.indexOf(",") > 0) {
			dataSource = pattern.split(dataSourceTable);
		} else {
			dataSource = new String[] { dataSourceTable };
		}

		for (int i = 0; i < dataSource.length; i++) {
			String source = dataSource[i];
			// String tarTableName = middels[j];
			if (source.indexOf("-") > 0) {
				String tarandsour[] = pattern1.split(source);
				// 数据源表名
				String middel = tarandsour[0];
				// 结果表名
				String tartable = tarandsour[1];
				// 查询要入库的表是否存在
				boolean tableFlag = pcDao.queryTableExist(tartable, targetBaseName);
				if (!tableFlag) {
					pcDao.createTable(tartable, targetBaseName);
				}

				
				 //查询现月份数据是否存在
				boolean dataFlag = pcDao.queryDataExist(tartable, targetBaseName, month);
				//存在则删除该月份数据
				if(dataFlag){
					pcDao.delMonthData(tartable, targetBaseName, month);
				}
				
				
				List<Map> listMap = new ArrayList<Map>();
				if (middel.endsWith("bandwidth") && tartable.equals("overview_kpi")) {
					//overview_kpi表数据
					 listMap = pcDao.getDataKpi(middel, dataSourceBaseName, tartable, month);
				} else if (middel.endsWith("bandwidth") && tartable.equals("overview_province")) {
					//overview_province表数据
					listMap = pcDao.getDataProvince(middel, dataSourceBaseName, tartable, month);
				} else if (middel.endsWith("operator") && tartable.equals("network_operator")) {
					//network_operator 表数据
					listMap = pcDao.getDataOperator(middel, dataSourceBaseName, tartable, month);
				}else if (middel.endsWith("operator") && tartable.equals("network_type")) {
					//network_type 表数据
					listMap = pcDao.getDataNetType(middel, dataSourceBaseName, tartable, month);
				} else if (middel.endsWith("terminal") && tartable.equals("terminal_os")) {
					//terminal_os 表数据
					listMap = pcDao.getDataTerminalOs(middel, dataSourceBaseName, tartable, month);
				} else if (middel.endsWith("terminal") && tartable.equals("terminal_model")) {
					//terminal_model 表数据
					listMap = pcDao.getDataTerminalModel(middel, dataSourceBaseName, tartable, month);
				}
				List<String> sqlList = new ArrayList<String>();
				if (listMap.size() > 0) {
					for (Map map : listMap) {
						String sql = pcDao.appendInsertSql(tartable, map);
						sqlList.add(sql);
					}
				}
				pcDao.dealSqlList(sqlList, targetBaseName);
				listMap.clear();
				sqlList.clear();
			}
		}

		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：： " + enddate + " ::  " + new Date() + "  ::用时：：：  " + (enddate - startdate));
	}
}
