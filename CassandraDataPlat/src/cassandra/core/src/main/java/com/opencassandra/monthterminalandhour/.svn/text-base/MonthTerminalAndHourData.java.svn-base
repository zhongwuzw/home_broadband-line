package com.opencassandra.monthterminalandhour;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.utils.UtilDate;

/**
 * 按月份 ，时段 ，终端 统计分布
 * 
 * @author：kxc
 * @date：Dec 14, 2015
 */
public class MonthTerminalAndHourData {

	// 配置code
	private static String[] codes = ConfParser.code;
	// 配置日期
	private static String[] tdates = ConfParser.testdate;
	// 表标识及表字段标识
	private static String[] pointertypes = ConfParser.pointertype;
	// 区域标识（uparea，area）
	private static String[] areacharts = ConfParser.areachart;
	// 数据入库，及分组标识（month，hour）
	private static String[] intervals = ConfParser.interval;
	//
	private static String subdatabase=ConfParser.subdatabase;

	// 该rex用于分解pointertypes
	private static Pattern pattern = Pattern.compile("[|]");
	private static Pattern pointpattern = Pattern.compile("[_]");
	
	private static MonthTerminalAndHourDao mAndHourDao = new MonthTerminalAndHourDao();

	public static void main(String[] args) {

		// 程序开始时间
		long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		startdate = new Date().getTime();
		System.out.println("开始进行数据读取::::::  " + startdate);
		// 循环code 可以直接获取数据源库名
		for (int i = 0; i < codes.length; i++) {
			String code = codes[i];
			// 查询获取数据源库名
			String srcBaseName = mAndHourDao.getDatabaseName(code);
			//拼接数据入库数据库表名
			String dstBaseName = subdatabase+"_"+code;
			// 循环时段标识
			for (int k = 0; k < intervals.length; k++) {
				String interval = intervals[k];

				//获取表标识点，若是包含'_'则只取第一个
				String mhinterval = "";
				if(interval.contains("_")){
					mhinterval= pointpattern.split(interval)[0];
				}else{
					mhinterval= interval;
				}
				
				// 循环日期
				for (int j = 0; j < tdates.length; j++) {
					String tdate = tdates[j];

					//如果没有date没有值，则取当前月份
					if(tdate.isEmpty()){
						tdate = UtilDate.getMonth();
					}
					/*//截取月份
					String month = tdate.substring(tdate.length()-2,tdate.length());
					//截取年份
					String year = tdate.substring(0,tdate.length()-2);*/
					// 循环表及字段标识
					for (int l = 0; l < pointertypes.length; l++) {
						String pointertype = pointertypes[l];
						// 分割标识字段
						String types[] = pattern.split(pointertype);
						// 表标识（ping,http,speed_test等）
						String type = types[0];
						// 字段标识（avg_rtt,avg_rate)
						String pointer = types[1];

						//获取表标识点，若是包含'_'则只取第一个
						String tabletype = "";
						if(type.contains("_")){
							tabletype= pointpattern.split(type)[0];
						}else{
							tabletype= type;
						}
						/**
						 * 2015年12月14日 15:55:21
						 * 该方法中的srcBaseName 库名有待确定
						 */
						//获取数据源表名
						String srctableName = mAndHourDao.getDataTableName(tabletype,tdate,srcBaseName);
						
						String dsttableName = type+"_"+interval+"_static";
						
						boolean flage = mAndHourDao.queryTableExist(dsttableName,dstBaseName);
						if(!flage){
							mAndHourDao.createTable(dsttableName,dstBaseName,mhinterval);
						}else{
							boolean dataFla = mAndHourDao.queryDataExist(dsttableName,dstBaseName,tdate);	
							if(dataFla){
								mAndHourDao.delMonthData(dsttableName,dstBaseName,tdate);
							}
							/*if(mhinterval.contains("month")){
								boolean dataFla = mAndHourDao.queryDataExist(dsttableName,dstBaseName,month,year);	
								if(dataFla){
									mAndHourDao.delMonthData(dsttableName,dstBaseName,month,year);
								}
							}*/
						}
						
						// 循环区域标识
						for (int m = 0; m < areacharts.length; m++) {
							String areachart = areacharts[m];
							//分割区域标识字段
							String areas[] = pattern.split(areachart);
							String uparea = "";
							String area = "";
							if(areas.length>1){
								// 上级区域字段 标识（）
								 uparea= areas[0];
								// 区域字段标识（）
								 area = areas[1];
							}else{
								area = areas[0];
							}
							
							List<Map> listMap = mAndHourDao.getData(srctableName,tdate,pointer,uparea,area,mhinterval,srcBaseName);
							List<String> sqlList = new ArrayList<String>();
							
							if(listMap.size()>0){
								
								//String id  = "";
								
								for (Map map : listMap) {
									
									String testtimes = (String) map.get("testtimes");
									String testvalues = (String) map.get("value");
									if(testtimes==null||"0".equals(testtimes)||testvalues==null){
										continue;
									}
									//String sql = "";
									String sql = mAndHourDao.appendInsertSql(dsttableName,map);
									/*if(map.containsKey("hour")){
										id = mAndHourDao.queryHourDataExist(dsttableName,dstBaseName,map);
									}
								
									if(map.containsKey("month")||id.isEmpty()){
										sql = mAndHourDao.appendInsertSql(dsttableName,map);
									}else{
										sql = mAndHourDao.appendUpdateSql(dsttableName,map,id);
									}*/
									
									sqlList.add(sql);
									/*if(sqlList.size()%20000==0){
										mAndHourDao.dealSqlList(sqlList,dstBaseName);
										listMap.clear();
										sqlList.clear();
									}*/
								}
							}
							
							mAndHourDao.dealSqlList(sqlList,dstBaseName);
							listMap.clear();
							sqlList.clear();
						}
					}
				}
			}
		}
		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：： " + enddate + " ::  " + new Date() + "  ::用时：：：  " + (enddate - startdate));
	}
}
