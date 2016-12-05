package com.opencassandra.jiakuan.servicequality.servicequalityorgid;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.jiakuan.servicequality.servicequalitybase.ServiceQualityHttpdownloadBseDao;

/**
 * 该程序可生产有关业务质量的统计数据（包括 orgid，operator及闲忙时表的数据）
 * 
 * @author kongxiangchun
 * 
 */
public class ServicequalityorgidHttpdownload {

	private static Pattern pattern = Pattern.compile("[,]");
	private static Pattern fieldpattern = Pattern.compile("[$]");
	private static Pattern fieldpattern1 = Pattern.compile("[|]");
	private static ServiceQualityHttpdownloadBseDao servicequalityhttpbasedao  = new ServiceQualityHttpdownloadBseDao();

	public static void main(String[] args) {

		
				String whereparameter = ConfParser.whereparameter;
				String groupparameter = ConfParser.groupparameter;
		
				// 程序开始时间
				long startdate = 0;
				// 程序结束时间
				long enddate = 0;
				startdate = new Date().getTime();
				System.out.println("开始进行数据读取::::::  " + startdate);
				//pc$test_map_jiakuan$pc_http_test, ,app$test_map_jiakuan$http_test_new_201609
				//String[] args = { "pc$test_map_jiakuan$pc_http_test_201609", "test_jiakuan", "servicequality_period_httpdownload", "auth", "201609" };

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
						boolean dataFlag = servicequalityhttpbasedao.queryDataExist(targetTable, targetBaseName, month,probetype);
						// 存在则删除该月份数据
						if (dataFlag) {
							servicequalityhttpbasedao.delMonthData(targetTable, targetBaseName, month,probetype);
						}
						List<Map> listMap = servicequalityhttpbasedao.getDataByprobetype(sourceTableName, sourceBaseName, targetTable, month, targetBaseName,probetype,authBaseName,whereparameter,groupparameter);
						if (listMap.size() > 0) {
							for (Map map : listMap) {
								String sql = servicequalityhttpbasedao.appendInsertSql(targetTable, map);
								sqlList.add(sql);
							}
						}
						servicequalityhttpbasedao.dealSqlList(sqlList, targetBaseName);
						listMap.clear();
						sqlList.clear();

						List<Map> groupList = ServiceQualityHttpdownloadBseDao.groupList;
						//groupList.clear();
						String[] dataArray = { "0.95,top95_download_rate|avg_rate", "0.85,top85_download_rate|avg_rate", "0.75,top75_download_rate|avg_rate"};

						if (groupList.size() > 0) {
							for (Map map : groupList) {

								for (int j = 0; j < dataArray.length; j++) {
								String data = dataArray[j];
								String[] pe = pattern.split(data);
								double percent = Double.parseDouble(pe[0]);
								String fields = pe[1];

								String[] field = fieldpattern1.split(fields);
								String tarField = field[0];
								String sourceField = field[1];


									String orgid = (String) map.get("orgid");
									String org = (String) map.get("org");
									String broadband_type = (String) map.get("broadband_type");
									String prtype = (String) map.get("probetype");
									int percentnum = (int) Math.ceil(Double.parseDouble((String) map.get("percentnum")) * percent) - 1;
									
									String gparameter = "";
									
									String setpar = "";
									
									if(groupparameter != null &&!groupparameter.isEmpty()){
										 gparameter = map.get(""+groupparameter+"")+"";
										setpar = " and "+groupparameter +"='" + gparameter + "'";
									}
									
									String wparameter = "";
									String setwpar = "";
									if( whereparameter != null && !whereparameter.isEmpty()){
										wparameter = map.get("period")+"";
										setwpar = " and period = '"+wparameter+"'";
									}
									
									String percentdata = servicequalityhttpbasedao.getPercentData(sourceTableName, sourceBaseName,authBaseName, month, orgid, broadband_type, percentnum, sourceField, tarField, targetBaseName,prtype,whereparameter,gparameter,wparameter);
									String sql = "update " + targetTable + " set "+tarField+"='"+percentdata+"'  where orgid='" + orgid + "' and broadband_type='" + broadband_type
											+ "' and month ='" + month + "' and probetype='"+prtype+"' "+setpar+" "+setwpar+"";
									System.out.println(sql);
									sqlList.add(sql);
							}
							}
							servicequalityhttpbasedao.dealSqlList(sqlList, targetBaseName);
							sqlList.clear();
							ServiceQualityHttpdownloadBseDao.groupList.clear();
						}

						
					}else{
						System.out.println("该数据源参数格式不对，请核对后重新启动程序");
					}
				}

		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：" + enddate + " ::  " + new Date() + "  ::用时：： " + (enddate - startdate));
	}
}