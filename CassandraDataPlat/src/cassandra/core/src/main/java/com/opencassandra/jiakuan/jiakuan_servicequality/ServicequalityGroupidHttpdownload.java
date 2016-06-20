package com.opencassandra.jiakuan.jiakuan_servicequality;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.jiakuan.jiakuan_servicequality.dao.ServicequalityGroupidHttpdownloadDao;

/**
 * 用於生成pc专线中servicequality_groupid_ping 表数据
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityGroupidHttpdownload {

	private static Pattern pattern = Pattern.compile("[,]");
	private static Pattern fieldpattern = Pattern.compile("[$]");
	private static ServicequalityGroupidHttpdownloadDao groupidHttpdownloadDao = new ServicequalityGroupidHttpdownloadDao();

	public static void main(String[] args1) {

		// 程序开始时间
				long startdate = 0;
				// 程序结束时间
				long enddate = 0;
				startdate = new Date().getTime();
				System.out.println("开始进行数据读取::::::  " + startdate);
				//pc$test_appreportdata_jiakuan$pc_http_test,
//				String[] args = { "pc$test_appreportdata_jiakuan$pc_http_test", "test_middle_table_jiakuan", "servicequality_groupid_httpdownload", "auth", "201605" };
				String[] args = { "app$test_appreportdata_jiakuan$http_test_new_201605", "test_middle_table_jiakuan", "servicequality_groupid_httpdownload", "auth", "201605" };

				for (int i = 0; i < args.length; i++) {
					System.out.println(args[i] + "   对应的参数");
				}

				// 探针类型$数据源库名$数据源表名 ($内部分割，类型之间','号分割)
				String dataSourceName = args[0];
				// 目标表所在库名
				String targetBaseName = args[1];
				// 目标表名
				String targetTable = args[2];

				String authBaseName = args[3];
				String month = args[4];
				String datasource[] = null;
				if (dataSourceName.indexOf(",") > 0) {
					datasource = pattern.split(dataSourceName);
				} else {
					datasource = new String[] { dataSourceName };
				}

				List<String> sqlList = new ArrayList<String>();
				for (int i = 0; i < datasource.length; i++) {
					String datasources = datasource[i];
					String[] sources = null;
					if (datasources.contains("$")) {
						sources = fieldpattern.split(datasources);
						// 探针类型
						String probetype = sources[0];
						// 数据源库名
						String sourceBaseName = sources[1];
						// 数据源表名
						String sourceTableName = sources[2];

						// 查询现月份数据是否存在
						boolean dataFlag = groupidHttpdownloadDao.queryDataExist(targetTable, targetBaseName, month,probetype);
						// 存在则删除该月份数据
						if (dataFlag) {
							groupidHttpdownloadDao.delMonthData(targetTable, targetBaseName, month,probetype);
						}
						List<Map> listMap = groupidHttpdownloadDao.getDataByprobetype(sourceTableName, sourceBaseName, targetTable, month, targetBaseName,probetype,authBaseName);
						if (listMap.size() > 0) {
							for (Map map : listMap) {
								String sql = groupidHttpdownloadDao.appendInsertSql(targetTable, map);
								sqlList.add(sql);
							}
						}
						groupidHttpdownloadDao.dealSqlList(sqlList, targetBaseName);
						listMap.clear();
						sqlList.clear();

						
					}else{
						System.out.println("该数据源参数格式不对，请核对后重新启动程序");
					}
				}
				
				
		/*List<Map> groupList = ServicequalityGroupidHttpdownloadDao.groupList;
		
		String[] dataArray = {"0.95,top95_download_rate|avg_delay","0.85,top95_download_rate|avg_delay","0.75,top95_download_rate|avg_delay"};
		
		if (groupList.size() > 0) {
			for (Map map : groupList) {
				
				for (int j = 0; j < dataArray.length; j++) {
					String data = dataArray[j];
					
					String[] pe = pattern.split(data);
					double percent = Double.parseDouble(pe[0]);
					String fields = pe[1];
					
					String[] field = fieldpattern.split(fields);
					String tarField = field[0];
					String sourceField = field[1];
					
					String groupid = (String) map.get("groupid");
					String broadband_type = (String) map.get("broadband_type");
					int percentnum =(int) Math.ceil( Double.parseDouble((String) map.get("percentnum"))*percent)-1;
					System.out.println(percentnum);
					
					
					String percentdata =groupidHttpuploadDao.getPercentData(dataSourceTable, dataSourceBaseName, month, groupid, broadband_type, percentnum,sourceField,tarField,targetBaseName);
					String sql ="update "+targetTable+" set "+tarField+" ='"+percentdata+"' where groupid='"+groupid+"' and broadband_type='"+broadband_type+"' and month ='"+month+"'";
					sqlList.add(sql);
				}
			}
			groupidHttpuploadDao.dealSqlList(sqlList, targetBaseName);
			sqlList.clear();
		}
		*/

		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：" + enddate + " ::  " + new Date() + "  ::用时：： " + (enddate - startdate));
	}
}
