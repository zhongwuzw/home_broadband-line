package com.opencassandra.jiakuan.jiakuan_servicequality;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.jiakuan.jiakuan_servicequality.dao.ServicequalityGroupidHttpuploadDao;
import com.opencassandra.jiakuan.jiakuan_servicequality.dao.ServicequalityorgidHttpuploadDao;

/**
 * 用於生成pc专线中servicequality_groupid_ping 表数据
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityorgidHttpupload {

	private static Pattern pattern = Pattern.compile("[,]");
	private static Pattern fieldpattern = Pattern.compile("[$]");
	private static ServicequalityorgidHttpuploadDao orgidHttpuploadDao = new ServicequalityorgidHttpuploadDao();

	public static void main(String[] args) {

		// 程序开始时间
				long startdate = 0;
				// 程序结束时间
				long enddate = 0;
				startdate = new Date().getTime();
				System.out.println("开始进行数据读取::::::  " + startdate);
				//pc$test_appreportdata_jiakuan$pc_http_test,
				//String[] args = { "app$test_appreportdata_jiakuan$http_test_new_201605", "test_middle_table_jiakuan", "servicequality_orgid_httpupload", "auth", "201605" };

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
						boolean dataFlag = orgidHttpuploadDao.queryDataExist(targetTable, targetBaseName, month,probetype);
						// 存在则删除该月份数据
						if (dataFlag) {
							orgidHttpuploadDao.delMonthData(targetTable, targetBaseName, month,probetype);
						}
						List<Map> listMap = orgidHttpuploadDao.getDataByprobetype(sourceTableName, sourceBaseName, targetTable, month, targetBaseName,probetype,authBaseName);
						if (listMap.size() > 0) {
							for (Map map : listMap) {
								String sql = orgidHttpuploadDao.appendInsertSql(targetTable, map);
								sqlList.add(sql);
							}
						}
						orgidHttpuploadDao.dealSqlList(sqlList, targetBaseName);
						listMap.clear();
						sqlList.clear();

						
					}else{
						System.out.println("该数据源参数格式不对，请核对后重新启动程序");
					}
				}
				
				
		/*List<Map> groupList = ServicequalityGroupidHttpuploadDao.groupList;
		
		String[] dataArray = {"0.95,top95_ping_delay|avg_delay","0.85,top85_ping_delay|avg_delay","0.75,top75_ping_delay|avg_delay","0.95,top95_ping_loss_rate|loss_rate","0.85,top85_ping_loss_rate|loss_rate","0.75,top75_ping_loss_rate|loss_rate"};
		
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
