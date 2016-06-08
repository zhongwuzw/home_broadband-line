package com.opencassandra.dealorgdata;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opencassandra.descfile.ConfParser;

public class DealOrgData {
	Statement statement;
	Connection conn;
	Format fm = new DecimalFormat("#.###");
	private List<String> sqlList = new ArrayList<String>();
	Map<String, ArrayList<ArrayList<String>>> ruleMap = new HashMap<String, ArrayList<ArrayList<String>>>();
	//List<String> table = new ArrayList<String>();
	List<String> tableNew = new ArrayList<String>();
	
	public static void main(String[] args1) {
		
		String[] args = {"testeventlogger_static","event_deal_data_rule","testeventlogger_1284","eventlogger_parsing_20160425","testeventlogger_1284","eventlogger_parsing__20160425_new"};
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i] +"   对应的参数值");
		}
		
		//规则表所在库名
		String ruleBaseName = args[0];
		//规则表表名
		String ruleTable = args[1];
		//数据源表所在库名
		String dataSourceBaseName = args[2];
		//数据源表名
		String dataSourceTable = args[3];
		//目标表所在库名
		String targetBaseName = args[4];
		//目标表名
		String targetTable = args[5];
		
		DealOrgData dealOrgData = new DealOrgData();
		dealOrgData.dealData(ruleBaseName,ruleTable,dataSourceBaseName,dataSourceTable,targetBaseName,targetTable);
	}
	
	
	/**
	 * 
	 * @param tableName 原表名
	 * @param newTableName 新表名
	 * @return void
	 */
	public void dealData(String ruleBaseName,String ruleTable,String dataSourceBaseName,String dataSourceTable,String targetBaseName,String targetTable){
		ruleMap.clear();
		//获取到对应清洗表中相应的对应规则，并以map<list> 的形式存放
		this.getRuleMap(ruleBaseName,ruleTable);
		try {
			//连接数据源库
			start(dataSourceBaseName);
			/**
			 * 2016年3月4日17:57:50
			 * 
			 * 注釋 
			 */
			/*String deleteSql = "DELETE a FROM  "+newTableName+" a where EXISTS (select * from "+tableName+" b where b.id=a.id and b.haschange=0);";
			statement.execute(deleteSql);*/
			String querySql = "select * from "+dataSourceTable+" where haschange=0  for update";
			ResultSet rs = statement.executeQuery(querySql);
			int num = 0;
			while(rs.next()){
				System.out.println("Start："+new Date().toLocaleString()+"&********************************");
				if(num<=500){
					num++;
				}else{
					this.dealSql(dataSourceBaseName,dataSourceTable,targetBaseName,targetTable,false);
					num = 0;
					sqlList.clear();
				}
				Map map = new HashMap();
				ResultSetMetaData rsmd = rs.getMetaData();
				int id = rs.getInt("id");
				if(id==0){
					continue;
				}
				int count = rsmd.getColumnCount();
				for (int i = 1; i <= count; i++) {
					String value = rs.getString(i);
					String name = rsmd.getColumnName(i);
					
					
					/**
					 * 2016年4月18日10:30:23
					 * 
					 * 修改 匹配有關模糊字段定義 數據規則，在規則中有一些通用的匹配規則，它對應著多個的字段名
					 * 這裡使用_來進行截取字符串前綴進行拼接匹配，再在後面進行數據放入map的過程中再把該字段名換回來
					 * 
					 */
					Map<String,String> columnMap = new HashMap<String,String>();
					
					if(ruleMap.containsKey(name)){
						columnMap = dealString(name,value);
					}else{
						String tname = "";
						if(name.contains("_")){
							tname = name.substring(0,name.indexOf("_")+1)+"*";
						}
						if(ruleMap.containsKey(tname)){
							columnMap = dealString(tname,value);
						}else{
							map.put(name, value);
						}
					}
					
					for (Iterator iterator = columnMap.keySet().iterator(); iterator
							.hasNext();) {
						String cname = (String) iterator.next();
						value = columnMap.get(cname);
						map.put(name, value);
					}
				}
				map.put("haschange", "1");
				this.updateData(map,targetTable,id);
				System.out.println("End："+new Date().toLocaleString()+"&********************************");
			}
			this.dealSql(dataSourceBaseName,dataSourceTable,targetBaseName,targetTable,true);
			sqlList.clear();
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	File file = new File("/home/mysqlInserttest/DealOrgData/error.txt");
	private void dealSql(String dataSourceBase,String dataSourceTable, String targetBase,String targetTable,boolean spec){
		try {
			start(targetBase);
			StringBuffer sql = new StringBuffer("");
			for (int i = 0; i < sqlList.size(); i++) {
				String updateSql = sqlList.get(i)==null?"":sqlList.get(i).toString();
				if(updateSql.isEmpty()){
					continue;
				}
				try {
					boolean flag = this.execute(updateSql);
					System.out.println("更新数据"+(flag==true?"成功":"失败"));
				} catch (Exception e) {
					System.out.println("更新数据失败：updateSql："+updateSql);
					e.printStackTrace();
				}
			}
			if(spec){
				if(dataSourceTable==null || dataSourceTable.isEmpty() || dataSourceTable==null || dataSourceTable.isEmpty()){
					
				}else{
					String updateSql = "update "+dataSourceBase+"."+dataSourceTable+" a set a.haschange =1 where a.haschange=0 and  EXISTS (select * from "+targetBase+"."+targetTable+" b where a.id=b.id );";
					statement.executeUpdate(updateSql);
				}
				
				/**
				 * 待修改静态sql处理问题
				 */
				
				//dealSpecSql(dataSourceBase,dataSourceTable,targetBase,targetTable);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private boolean dealSpecSql(String dataSourceBase,String dataSourceTable, String targetBase,String targetTable,String month) {
		boolean flag = true;
		String sql= "";
		ArrayList<String> list = new ArrayList<String>();
		
		if(dataSourceTable.startsWith("ping")&&targetTable.startsWith("ping_new")){
			sql = "update ping_new_"+month+" set normalornot = (case when(signal_strength = '' or signal_strength is null) or (max_rtt is not null and min_rtt is not null and avg_rtt is not null and success_rate = '0') or success_rate='--'  then 'no' else 'yes' end)";
			list.add(sql);
			
		}
		
		//---------------------------------------------------speed-------------------------------------------------------------
		sql = "delete from speed_test_new_"+month+" where delay is NULL and max_up is null and max_down is null ; ";
		list.add(sql);
		sql = "update speed_test_new_"+month+" set normalornot =  (case when signal_strength = '' or signal_strength is null or avg_up=0  THEN 'no'  else 'yes'  end) ";
		list.add(sql);
		
		//---------------------------------------------------http_test----------------------------------------------------------
		
		sql = "update http_test_new_"+month+" set resource_size = SUBSTRING_INDEX(SUBSTRING_INDEX(resource_size, '/', -1),'M',1)";
		list.add(sql);
		sql = "update http_test_new_"+month+" set avg_rate = avg_rate/1024, max_rate = max_rate/1024  WHERE  avg_rate>100  and  max_rate>100 ";
		list.add(sql);
		sql = "update http_test_new_"+month+" set normalornot = (case when signal_strength = '' or signal_strength is null or avg_rate>100  THEN 'no'  else 'yes'  end)";
		list.add(sql);
		sql = "delete from http_test_new_"+month+" where file_type is null;";
		list.add(sql);
		
		
		/*sql = "update http_test_new_1 set max_rate = (case  when  max_rate>100 or avg_rate>100 then  max_rate/1024 else max_rate end)";
		list.add(sql);
		sql = "update http_test_new set avg_rate = (case  when (net_type not like '%WIFI%' and  net_type not like '%LTE%') or avg_rate>100 then  avg_rate/1024 else avg_rate end)";
		list.add(sql);
		sql = "update http_test_new set max_rate = (case  when (net_type not like '%WIFI%' and net_type not like '%LTE%') or max_rate>100 then  max_rate/1024 else max_rate end)";
		list.add(sql);
		 */		
		
		
		//-------------------------------------------------------web_browsing------------------------------------------------------
		sql = "update web_browsing_new_"+month+" set normalornot = (case when (signal_strength = '' or signal_strength is null) or (reference=0 and success_rate = '100')  THEN 'no' else 'yes' end)";
		list.add(sql);
		//-----------------------------------------------hand_over--------------------------------------------------------------
		sql = "update hand_over_new_"+month+" set normalornot = (case when signal_strength = '' or signal_strength is null  THEN 'no'   else 'yes'   end)";
		list.add(sql);
		
		//-------------------------------------------------call_test------------------------------------------------------------
		sql = "DELETE a from  call_test_new_"+month+" a where EXISTS (select * from call_test_"+month+" b where b.id=a.id and b.haschange=0);";
		list.add(sql);
		sql = "update call_test_new_"+month+" set normalornot = (case when signal_strength = '' or signal_strength is null  THEN 'no'  else 'yes'  end)";
		list.add(sql);
		//------------------------------------------------------sms-------------------------------------------------------

		sql = "update sms_new_"+month+" set normalornot = (case when (signal_strength = '' or signal_strength is null) or (sending_delay = NULL and success_rate='100')  then 'no'  else 'yes'  end)";
		list.add(sql);
		
		//--------------------------------------------------------------video_test-----------------------------------------------
		
		
		//--------------------------------------------------------sms_query-----------------------------------------------------
		
		sql = "update sms_query_new_"+month+" set  normalornot = (case when (signal_strength = '' or signal_strength is null) or (sending_delay=0 and success_rate='100%')  then 'no'  else 'yes'  end)";
		list.add(sql);
		//-------------------------------------------------------------------------------------------------------------

		
		
		
		
		
		
	
		
	
		
		
		
		
		for (int i = 0; i < list.size(); i++) {
			String sqlString = list.get(i);
			try {
				statement.execute(sqlString);
			} catch (SQLException e) {
				e.printStackTrace();
				continue;
			}
		}
		return flag;
	}
	private void updateData(Map map,String targetTable,Integer id){
		StringBuffer sql = new StringBuffer("");
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		String table = "";
		if(targetTable==null || targetTable.isEmpty()){
			System.out.println("表名为空");
			return ;
		}else{
			table = targetTable;
		}
		sql.append("insert into "+table+" ");
		try {
			Set set = map.keySet();
			Iterator iter = set.iterator();
			while(iter.hasNext()){
				String name = (String)iter.next();
				String value = map.get(name)==null?"":map.get(name).toString();
				if(value.isEmpty() || value.equals("--") || value.equals("--/--") || value.equals("N/A") || value.equals("null") ){
					continue;
				}else if(isNum(value)){
					
				}else {
					value = "'"+value+"'";
				}
				columnStr.append(name).append(",");
				valueStr.append(value).append(",");
			}
			while(columnStr.toString().trim().endsWith(",")){
				columnStr = new StringBuffer(columnStr.toString().substring(0,columnStr.toString().lastIndexOf(",")));
			}
			while(valueStr.toString().trim().endsWith(",")){
				valueStr = new StringBuffer(valueStr.toString().substring(0,valueStr.toString().lastIndexOf(",")));
			}
			columnStr.append(" )");
			valueStr.append(" )");
			sql.append(columnStr);
			sql.append(valueStr);
			System.out.println(sql.toString());
			sqlList.add(sql.toString());
		} catch (Exception e) {
			System.out.println("拼装语句错误："+sql);
			sql = new StringBuffer("");
			e.printStackTrace();
		}
	}
	public void getRuleMap(String ruleBase,String ruleTable) {
		
		//tableName = tableName.replace("_"+month, "");
		
		try {
			//start();
			String url = ConfParser.url+"/"+ruleBase;
			getstart(url, ConfParser.user, ConfParser.password);
			String sql = "select * from "+ruleTable+"";
			try {
				Map<String, ArrayList<String>> resultMap = new HashMap<String, ArrayList<String>>();
				ResultSet rs = statement.executeQuery(sql);
				ResultSetMetaData rsmd = rs.getMetaData();
				int num = 0;
				while (rs.next()) {
					ArrayList<String> list = new ArrayList<String>();
					// 规则表中数据 ： 表名 新表名 源字段 新字段 匹配规则 匹配字符 处理规则替换字符 单位换算
					num ++;
					int id = rs.getInt("id");
					if (id == 0) {
						continue;
					}
					int count = rsmd.getColumnCount();
					//根据许勤洗的表名获取清洗数据库表中的该表明的所有规则，并把每一条数据做为元素放入list中
					for (int i = 1; i <= count; i++) {
						String value = rs.getString(i);
						String name = rsmd.getColumnName(i);
						
						System.out.println(name +"  :::name::  "+value+"  :::value");
						/*if (name.equals("table_name")) {
							table_name = value;
							resultMap.put("table_name", table_name);
						}else*/ 
						if(name.equals("id")){
							
						}else {
							list.add(value);
						}
					}
					if (list.size() <= 0) {

					} else {
						resultMap.put(num+"", list);
					}
				}
				
				//
				for (int i = 1; i <= resultMap.size(); i++) {
					ArrayList<String> list = resultMap.get(i+"");
					String column = list.get(0).toString();
					ArrayList<ArrayList<String>> resultlist = new ArrayList<ArrayList<String>>();
					if(ruleMap.containsKey(column)){
						resultlist = ruleMap.get(column);
					}
					resultlist.add(list);
					ruleMap.put(column, resultlist);
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	

	
	public Map<String,String> dealString(String name,String str){
		Map map = new HashMap<String,String>();
		boolean flag = false; //判断当前字段值是否经过符合条件的处理 （仅适用不同字段的情况）
		if(!ruleMap.containsKey(name)){
			map.put(name, str);
			return map;
		}
		
		
		/**
		 * 该处的name为表名即根据该表名找出对应的清洗规则所对应的list
		 */
		
		ArrayList<ArrayList<String>> resultList = ruleMap.get(name);
		if(resultList==null || resultList.size()<=0){
			map.put(name, str);
			return map;
		}
		str = str==null?null:str;
		String result = str;
		/**
		 * 可修改
		 */
		for (int count = 0; count < resultList.size(); count++) {
			List<String> ruleList = resultList.get(count);
			String new_column = ruleList.get(1);
			String matching_rule = ruleList.get(2);
			String matching_char = ruleList.get(3);
			
			String processing_rule = ruleList.get(4);
			String replace_char = ruleList.get(5);
			String conversion = ruleList.get(6);
			
			if(matching_rule.equals("like")){
				if(matching_char == null){
					if (str == null) {
						if(!name.equals(new_column)){
							map.put(name, str);
							flag = true;
						}
						if (processing_rule.equals("remove")) {
							str = "";
						} else if (processing_rule.equals("replace")) {
							str = replace_char;
						}
					}else{
						if(flag){
							
						}else{
							if(!name.equals(new_column)){
								map.put(name, str);
							}
						}
					}
				}else{
					if(matching_char.contains("|")){
						String []matching_chars = matching_char.split("\\|");
						String []conversions ;
						if(conversion==null||conversion.isEmpty()){
							conversions = new String[]{};
						}else{
							conversions = conversion.split("\\|");
						}
						for (int i = 0; i < matching_chars.length; i++) {
							String matchingCharString = matching_chars[i];
							String conversionString = "";
							if ((conversions.length != matching_chars.length && i < conversions.length)
									|| (conversions.length == matching_chars.length)){
								conversionString = conversions[i];
							}
							if (str != null && str.contains(matchingCharString)) {
								if(!name.equals(new_column)){
									map.put(name, str);
									flag = true;
								}
								if (processing_rule.equals("remove")) {
									str = str.replace(matchingCharString, "");
								} else if (processing_rule.equals("replace")) {
									str = replace_char==null?str:str.replace(matchingCharString, replace_char);
								}
								if (conversion != null && !conversion.isEmpty()
										&& str!=null && isNum(str)) {
									if(conversionString.isEmpty()){
										continue;
									}
									String conversionType = conversionString.substring(0,1);
									String conversionNum = conversionString.substring(1,conversionString.length());
									if(conversionType.equals("+")){
										str = Double.parseDouble(str)
										+ Integer.parseInt(conversionNum) + "";
									}else if (conversionType.equals("-")) {
										str = Double.parseDouble(str)
										- Integer.parseInt(conversionNum) + "";
									}else if (conversionType.equals("*")) {
										str = Double.parseDouble(str)
										* Integer.parseInt(conversionNum) + "";
									}else if (conversionType.equals("/")) {
										str = Double.parseDouble(str)
										/ Integer.parseInt(conversionNum) + "";	
									}
								}
							}else{
								if(str == null){
									if(matchingCharString == null){
										if(!name.equals(new_column)){
											map.put(name, str);
											flag = true;
										}
										if (processing_rule.equals("remove")) {
											str = "";
										} else if (processing_rule.equals("replace")) {
											str = replace_char;
										}
										if (conversion != null && !conversion.isEmpty()
												&& str!=null && isNum(str)) {
											if(conversionString.isEmpty()){
												continue;
											}
											String conversionType = conversionString.substring(0,1);
											String conversionNum = conversionString.substring(1,conversionString.length());
											if(conversionType.equals("+")){
												str = Double.parseDouble(str)
												+ Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("-")) {
												str = Double.parseDouble(str)
												- Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("*")) {
												str = Double.parseDouble(str)
												* Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("/")) {
												str = Double.parseDouble(str)
												/ Integer.parseInt(conversionNum) + "";	
											}
										}
									}else{
										if(flag){
											
										}else{
											if(!name.equals(new_column)){
												map.put(name, str);
											}
										}
									}
								}else{
									if(flag){
										
									}else{
										if(!name.equals(new_column)){
											map.put(name, str);
										}
									}
								}
							}
						}
					}else{
						if(str == null){
							if(flag){
								
							}else{
								if(!name.equals(new_column)){
									map.put(name, str);
								}
							}
						}else{
							if (str.contains(matching_char)) {
								if(!name.equals(new_column)){
									map.put(name, str);
									flag = true;
								}
								if (processing_rule.equals("remove")) {
									str = str.replace(matching_char, "");
								} else if (processing_rule.equals("replace")) {
									str = replace_char==null?str:str.replace(matching_char, replace_char);
								}
								if (conversion != null && !conversion.isEmpty()
										&& str!=null && isNum(str)) {
									if(conversion.isEmpty()){
									}else{
										String conversionType = conversion.substring(0,1);
										String conversionNum = conversion.substring(1,conversion.length());
										if(conversionType.equals("+")){
											str = Double.parseDouble(str)
											+ Integer.parseInt(conversionNum) + "";
										}else if (conversionType.equals("-")) {
											str = Double.parseDouble(str)
											- Integer.parseInt(conversionNum) + "";
										}else if (conversionType.equals("*")) {
											str = Double.parseDouble(str)
											* Integer.parseInt(conversionNum) + "";
										}else if (conversionType.equals("/")) {
											str = Double.parseDouble(str)
											/ Integer.parseInt(conversionNum) + "";	
										}
									}
								}
							}else{
								if(flag){
								}else{
									if(!name.equals(new_column)){
										map.put(name, str);
									}
								}
							}
						}
					}
				}
			}else if(matching_rule.equals("=")){
				if(matching_char!=null){
					if(matching_char.contains("|")){
						String matching_chars[] = matching_char.split("\\|");
						String []conversions ;
						if(conversion==null||conversion.isEmpty()){
							conversions = new String[]{};
						}else{
							conversions = conversion.split("\\|");
						}
						for (int i = 0; i < matching_chars.length; i++) {
							String match_char = matching_chars[i];
							String conversionString = "";
							if ((conversions.length != matching_chars.length && i < conversions.length)
									|| (conversions.length == matching_chars.length)){
								conversionString = conversions[i];
							}
							if(str == null){
								if(match_char == null){
									if(!name.equals(new_column)){
										map.put(name, str);
										flag = true;
									}
									if(processing_rule.equals("remove")){
										str = "";
									}else if(processing_rule.equals("replace")){
										str = replace_char;
									}
									if (conversion != null && !conversion.isEmpty()
											&& str!=null && isNum(str)) {
										if(conversion.isEmpty()){
										}else{
											String conversionType = conversion.substring(0,1);
											String conversionNum = conversion.substring(1,conversion.length());
											if(conversionType.equals("+")){
												str = Double.parseDouble(str)
												+ Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("-")) {
												str = Double.parseDouble(str)
												- Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("*")) {
												str = Double.parseDouble(str)
												* Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("/")) {
												str = Double.parseDouble(str)
												/ Integer.parseInt(conversionNum) + "";	
											}
										}
									}
								}else{
									if(flag){
										
									}else{
										if(!name.equals(new_column)){
											map.put(name, str);
										}	
									}
								}
							}else{
								if(match_char == null){
									if(flag){
										
									}else{
										if(!name.equals(new_column)){
											map.put(name, str);
										}	
									}
								}else{
									if(str.equals(match_char)){
										if(!name.equals(new_column)){
											map.put(name, str);
											flag = true;
										}
										if(processing_rule.equals("remove")){
											str = "";
										}else if(processing_rule.equals("replace")){
											str = replace_char;
										}
										if (conversion != null && !conversion.isEmpty()
												&& str!=null && isNum(str)) {
											if(conversion.isEmpty()){
											}else{
												String conversionType = conversion.substring(0,1);
												String conversionNum = conversion.substring(1,conversion.length());
												if(conversionType.equals("+")){
													str = Double.parseDouble(str)
													+ Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("-")) {
													str = Double.parseDouble(str)
													- Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("*")) {
													str = Double.parseDouble(str)
													* Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("/")) {
													str = Double.parseDouble(str)
													/ Integer.parseInt(conversionNum) + "";	
												}
											}
										}
									}else{
										if(flag){
											
										}else{
											if(!name.equals(new_column)){
												map.put(name, str);
											}	
										}
									}
								}
							}
						}
					} else {
						if (str == null) {
							if (flag) {

							} else {
								if (!name.equals(new_column)) {
									map.put(name, str);
								}
							}
						} else {
							if (str.equals(matching_char)) {
								if (!name.equals(new_column)) {
									map.put(name, str);
									flag = true;
								}
								if (processing_rule.equals("remove")) {
									str = "";
								} else if (processing_rule.equals("replace")) {
									str = replace_char;
								}
								if (conversion != null && !conversion.isEmpty()
										&& str != null && isNum(str)) {
									if (conversion.isEmpty()) {
									} else {
										String conversionType = conversion
												.substring(0, 1);
										String conversionNum = conversion
												.substring(1, conversion
														.length());
										if (conversionType.equals("+")) {
											str = Double.parseDouble(str)
													+ Integer
															.parseInt(conversionNum)
													+ "";
										} else if (conversionType.equals("-")) {
											str = Double.parseDouble(str)
													- Integer
															.parseInt(conversionNum)
													+ "";
										} else if (conversionType.equals("*")) {
											str = Double.parseDouble(str)
													* Integer
															.parseInt(conversionNum)
													+ "";
										} else if (conversionType.equals("/")) {
											str = Double.parseDouble(str)
													/ Integer
															.parseInt(conversionNum)
													+ "";
										}
									}
								}
							} else {
								if (flag) {

								} else {
									if (!name.equals(new_column)) {
										map.put(name, str);
									}
								}
							}
						}
					}
				}else{
					if(str == null){
						if(!name.equals(new_column)){
							map.put(name, str);
							flag = true;
						}
						if(processing_rule.equals("remove")){
							str = "";
						}else if(processing_rule.equals("replace")){
							str = replace_char;
						}
						if (conversion != null && !conversion.isEmpty()
								&& str!=null && isNum(str)) {
							if(conversion.isEmpty()){
							}else{
								String conversionType = conversion.substring(0,1);
								String conversionNum = conversion.substring(1,conversion.length());
								if(conversionType.equals("+")){
									str = Double.parseDouble(str)
									+ Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("-")) {
									str = Double.parseDouble(str)
									- Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("*")) {
									str = Double.parseDouble(str)
									* Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("/")) {
									str = Double.parseDouble(str)
									/ Integer.parseInt(conversionNum) + "";	
								}
							}
						}
					}else{
						if(flag){
							
						}else{
							if(!name.equals(new_column)){
								map.put(name, str);
							}	
						}
					}
				}
			}else if(matching_rule.equals(">")){
				if(matching_char!=null){
					if(matching_char.contains("|")){
						String matching_chars[] = matching_char.split("\\|");
						String []conversions ;
						if(conversion==null||conversion.isEmpty()){
							conversions = new String[]{};
						}else{
							conversions = conversion.split("\\|");
						}
						for (int i = 0; i < matching_chars.length; i++) {
							String match_char = matching_chars[i];
							String conversionString = "";
							if ((conversions.length != matching_chars.length && i < conversions.length)
									|| (conversions.length == matching_chars.length)){
								conversionString = conversions[i];
							}
							if(str == null){
								if(match_char == null){
									if(!name.equals(new_column)){
										map.put(name, str);
										flag = true;
									}
									if(processing_rule.equals("remove")){
										str = "";
									}else if(processing_rule.equals("replace")){
										str = replace_char;
									}
									if (conversion != null && !conversion.isEmpty()
											&& str!=null && isNum(str)) {
										if(conversion.isEmpty()){
										}else{
											String conversionType = conversion.substring(0,1);
											String conversionNum = conversion.substring(1,conversion.length());
											if(conversionType.equals("+")){
												str = Double.parseDouble(str)
												+ Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("-")) {
												str = Double.parseDouble(str)
												- Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("*")) {
												str = Double.parseDouble(str)
												* Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("/")) {
												str = Double.parseDouble(str)
												/ Integer.parseInt(conversionNum) + "";	
											}
										}
									}
								}else{
									if(flag){
										
									}else{
										if(!name.equals(new_column)){
											map.put(name, str);
										}	
									}
								}
							}else{
								if(match_char == null){
									if(flag){
										
									}else{
										if(!name.equals(new_column)){
											map.put(name, str);
										}	
									}
								}else{
									
									if (str!=null && isNum(str)) {
									
									if(Double.parseDouble(str) > Double.parseDouble(match_char)){
										if(!name.equals(new_column)){
											map.put(name, str);
											flag = true;
										}
										if(processing_rule.equals("remove")){
											str = "";
										}else if(processing_rule.equals("replace")){
											str = replace_char;
										}
										if (conversion != null && !conversion.isEmpty()
												&& str!=null && isNum(str)) {
											if(conversion.isEmpty()){
											}else{
												String conversionType = conversion.substring(0,1);
												String conversionNum = conversion.substring(1,conversion.length());
												if(conversionType.equals("+")){
													str = Double.parseDouble(str)
													+ Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("-")) {
													str = Double.parseDouble(str)
													- Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("*")) {
													str = Double.parseDouble(str)
													* Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("/")) {
													str = Double.parseDouble(str)
													/ Integer.parseInt(conversionNum) + "";	
												}
											}
										}
									}
									}else{
										if(flag){
											
										}else{
											if(!name.equals(new_column)){
												map.put(name, str);
											}	
										}
									}
								}
							}
						}
					} else {
						if (str == null) {
							if (flag) {

							} else {
								if (!name.equals(new_column)) {
									map.put(name, str);
								}
							}
						} else {
							if(str != null && !str.isEmpty()&& isNum(str)){
								if (Double.parseDouble(str)>Double.parseDouble(matching_char)) {
									if (!name.equals(new_column)) {
										map.put(name, str);
										flag = true;
									}
									if (processing_rule.equals("remove")) {
										str = "";
									} else if (processing_rule.equals("replace")) {
										str = replace_char;
									}
									if (conversion != null && !conversion.isEmpty()
											&& str != null && isNum(str)) {
										if (conversion.isEmpty()) {
										} else {
											String conversionType = conversion
													.substring(0, 1);
											String conversionNum = conversion
													.substring(1, conversion
															.length());
											if (conversionType.equals("+")) {
												str = Double.parseDouble(str)
														+ Integer
																.parseInt(conversionNum)
														+ "";
											} else if (conversionType.equals("-")) {
												str = Double.parseDouble(str)
														- Integer
																.parseInt(conversionNum)
														+ "";
											} else if (conversionType.equals("*")) {
												str = Double.parseDouble(str)
														* Integer
																.parseInt(conversionNum)
														+ "";
											} else if (conversionType.equals("/")) {
												str = Double.parseDouble(str)
														/ Integer
																.parseInt(conversionNum)
														+ "";
											}
										}
									}
								} else {
									if (flag) {

									} else {
										if (!name.equals(new_column)) {
											map.put(name, str);
										}
									}
								}
							}
							}
							
					}
				}else{
					if(str == null){
						if(!name.equals(new_column)){
							map.put(name, str);
							flag = true;
						}
						if(processing_rule.equals("remove")){
							str = "";
						}else if(processing_rule.equals("replace")){
							str = replace_char;
						}
						if (conversion != null && !conversion.isEmpty()
								&& str!=null && isNum(str)) {
							if(conversion.isEmpty()){
							}else{
								String conversionType = conversion.substring(0,1);
								String conversionNum = conversion.substring(1,conversion.length());
								if(conversionType.equals("+")){
									str = Double.parseDouble(str)
									+ Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("-")) {
									str = Double.parseDouble(str)
									- Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("*")) {
									str = Double.parseDouble(str)
									* Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("/")) {
									str = Double.parseDouble(str)
									/ Integer.parseInt(conversionNum) + "";	
								}
							}
						}
					}else{
						if(flag){
							
						}else{
							if(!name.equals(new_column)){
								map.put(name, str);
							}	
						}
					}
				}
			}else if (matching_rule.equals("<")){

				if(matching_char!=null){
					if(matching_char.contains("|")){
						String matching_chars[] = matching_char.split("\\|");
						String []conversions ;
						if(conversion==null||conversion.isEmpty()){
							conversions = new String[]{};
						}else{
							conversions = conversion.split("\\|");
						}
						for (int i = 0; i < matching_chars.length; i++) {
							String match_char = matching_chars[i];
							String conversionString = "";
							if ((conversions.length != matching_chars.length && i < conversions.length)
									|| (conversions.length == matching_chars.length)){
								conversionString = conversions[i];
							}
							if(str == null){
								if(match_char == null){
									if(!name.equals(new_column)){
										map.put(name, str);
										flag = true;
									}
									if(processing_rule.equals("remove")){
										str = "";
									}else if(processing_rule.equals("replace")){
										str = replace_char;
									}
									if (conversion != null && !conversion.isEmpty()
											&& str!=null && isNum(str)) {
										if(conversion.isEmpty()){
										}else{
											String conversionType = conversion.substring(0,1);
											String conversionNum = conversion.substring(1,conversion.length());
											if(conversionType.equals("+")){
												str = Double.parseDouble(str)
												+ Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("-")) {
												str = Double.parseDouble(str)
												- Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("*")) {
												str = Double.parseDouble(str)
												* Integer.parseInt(conversionNum) + "";
											}else if (conversionType.equals("/")) {
												str = Double.parseDouble(str)
												/ Integer.parseInt(conversionNum) + "";	
											}
										}
									}
								}else{
									if(flag){
										
									}else{
										if(!name.equals(new_column)){
											map.put(name, str);
										}	
									}
								}
							}else{
								if(match_char == null){
									if(flag){
										
									}else{
										if(!name.equals(new_column)){
											map.put(name, str);
										}	
									}
								}else{
									
									if (str!=null && isNum(str)) {
									
									if(Double.parseDouble(str) < Double.parseDouble(match_char)){
										if(!name.equals(new_column)){
											map.put(name, str);
											flag = true;
										}
										if(processing_rule.equals("remove")){
											str = "";
										}else if(processing_rule.equals("replace")){
											str = replace_char;
										}
										if (conversion != null && !conversion.isEmpty()
												&& str!=null && isNum(str)) {
											if(conversion.isEmpty()){
											}else{
												String conversionType = conversion.substring(0,1);
												String conversionNum = conversion.substring(1,conversion.length());
												if(conversionType.equals("+")){
													str = Double.parseDouble(str)
													+ Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("-")) {
													str = Double.parseDouble(str)
													- Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("*")) {
													str = Double.parseDouble(str)
													* Integer.parseInt(conversionNum) + "";
												}else if (conversionType.equals("/")) {
													str = Double.parseDouble(str)
													/ Integer.parseInt(conversionNum) + "";	
												}
											}
										}
									}
									}else{
										if(flag){
											
										}else{
											if(!name.equals(new_column)){
												map.put(name, str);
											}	
										}
									}
								}
							}
						}
					} else {
						System.out.println(str);
						if (str == null) {
							if (flag) {

							} else {
								if (!name.equals(new_column)) {
									map.put(name, str);
								}
							}
						} else {
							if(str != null && !str.isEmpty()&& isNum(str)){
							if (Double.parseDouble(str)<Double.parseDouble(matching_char)) {
								if (!name.equals(new_column)) {
									map.put(name, str);
									flag = true;
								}
								if (processing_rule.equals("remove")) {
									str = "";
								} else if (processing_rule.equals("replace")) {
									str = replace_char;
								}
								if (conversion != null && !conversion.isEmpty()
										&& str != null && isNum(str)) {
									if (conversion.isEmpty()) {
									} else {
										String conversionType = conversion
												.substring(0, 1);
										String conversionNum = conversion
												.substring(1, conversion
														.length());
										if (conversionType.equals("+")) {
											str = Double.parseDouble(str)
													+ Integer
															.parseInt(conversionNum)
													+ "";
										} else if (conversionType.equals("-")) {
											str = Double.parseDouble(str)
													- Integer
															.parseInt(conversionNum)
													+ "";
										} else if (conversionType.equals("*")) {
											str = Double.parseDouble(str)
													* Integer
															.parseInt(conversionNum)
													+ "";
										} else if (conversionType.equals("/")) {
											str = Double.parseDouble(str)
													/ Integer
															.parseInt(conversionNum)
													+ "";
										}
									}
								}
							} else {
								if (flag) {

								} else {
									if (!name.equals(new_column)) {
										map.put(name, str);
									}
								}
							}
							}
						}
					}
				}else{
					if(str == null){
						if(!name.equals(new_column)){
							map.put(name, str);
							flag = true;
						}
						if(processing_rule.equals("remove")){
							str = "";
						}else if(processing_rule.equals("replace")){
							str = replace_char;
						}
						if (conversion != null && !conversion.isEmpty()
								&& str!=null && isNum(str)) {
							if(conversion.isEmpty()){
							}else{
								String conversionType = conversion.substring(0,1);
								String conversionNum = conversion.substring(1,conversion.length());
								if(conversionType.equals("+")){
									str = Double.parseDouble(str)
									+ Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("-")) {
									str = Double.parseDouble(str)
									- Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("*")) {
									str = Double.parseDouble(str)
									* Integer.parseInt(conversionNum) + "";
								}else if (conversionType.equals("/")) {
									str = Double.parseDouble(str)
									/ Integer.parseInt(conversionNum) + "";	
								}
							}
						}
					}else{
						if(flag){
							
						}else{
							if(!name.equals(new_column)){
								map.put(name, str);
							}	
						}
					}
				}
			
				
			}else if(matching_rule.equals("NotNumber")){
				if(str==null||str.isEmpty()){
					
				}else{
				if(!isNum(str)){
					if(processing_rule.equals("remove")){
						str = "";
					}else if(processing_rule.equals("replace")){
						str = replace_char;
					}
				}
				}
			}
			
			if(new_column==null || new_column.isEmpty()){
				map.put(name, str);
			}else{
				if(flag){
					map.put(new_column, str);	
				}else{
					map.put(name, str);
				}
			}
			if(new_column.equals(name) && name.equals("resource_size") && str!=null && str.contains("/") && str.contains("M")){
				str = str.substring(str.lastIndexOf("/")+1, str.lastIndexOf("M"));
			}
		}
		return map; 
	}
	
	
	public void getstart(String url, String user, String password) throws Exception {
		System.out.println(url + " :数据库连接: " + user + " :: " + password);
		this.close();
		String driver = "com.mysql.jdbc.Driver";
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
	}
	
	
	public void start(String baseName) throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		// String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
		String url = ConfParser.url+"/"+baseName;
		System.out.println("******************"+url);
		String user = ConfParser.user;
		// String password = "cmrictpdata";
		String password = ConfParser.password;
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
	}

	public void close() throws SQLException {
		if(statement!=null){
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	public boolean execute(String sql) {
		System.out.println("sql:" + sql);
		boolean flag = true;
		if(sql.isEmpty()){
			flag = false;
		}else{
			try {
				statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				flag = false;
			}
		}
		return flag;
	}
	public boolean isNum(String str){
		return str.matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$");
	}
}

