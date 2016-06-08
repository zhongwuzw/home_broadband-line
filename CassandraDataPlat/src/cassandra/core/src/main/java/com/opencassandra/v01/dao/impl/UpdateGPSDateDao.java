package com.opencassandra.v01.dao.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;

public class UpdateGPSDateDao {

	double m_maxspeed = 150;
	double m_opacitymax = 0.1;
	double m_opacitymin = 0.8;
	double m_maxtestnum = 20;
	Connection conn ;
	InsertMysqlByDataToGPS insertMysqlByDataToGPS = new InsertMysqlByDataToGPS();
	
	public static void main(String[] args) {
	}
	/**
	 * 更新http_201509_1类的数据
	 * @param tableName http_201509_1
	 * @param start
	 * @param end
	 * @return void
	 */
	public void updateGPS(String tableName,int start,int end){
		conn = getConn();
		String spaceGpsStr = "";
		String latitude = "";
		String longitude = "";
		String resp = "";
		String dbProvince = "";
		String dbDistrict = "";
		String dbCity = "";
		String fillred = "";
		String fillgreen = "";
		String fillOpacity = "";
		
		
		int testCounts = 0;
		double avg_rate = 0.0;
		double top = 0.0;
		double down = 0.0;
		String provinceString = "";
		String districtString = "";
		String cityString = "";
		String street = "";
		String streetNum = "";
		String cityCode = "";
		Map map = new HashMap();
		Statement statement = null;
		boolean flag = true;
		try {
			while (flag) {
				statement = getStat();
				String sql = "select * from " + tableName
						+ " order by id limit " + start + "," + end + " ";
				ResultSet rs = statement.executeQuery(sql);
				flag = false;
				while (rs.next()) {
					flag = true;
					longitude = rs.getString("lng");
					latitude = rs.getString("lat");
					dbProvince = rs.getString("province") == null ? "" : rs
							.getString("province");
					dbCity = rs.getString("city") == null ? "" : rs
							.getString("city");
					dbDistrict = rs.getString("district") == null ? "" : rs
							.getString("district");

					testCounts = rs.getInt("test_times");
					if(tableName.startsWith("speed_test")){
						top = rs.getDouble("top_rate");
						down = rs.getDouble("down_rate");
					}else{
						avg_rate = rs.getDouble("avg_rate");	
					}
					//只更新数据库中地理位置为空的数据
					if(!dbProvince.isEmpty() && dbProvince.length()>1){
						continue;
					}
					try {
						resp = testPost(latitude, longitude);
					} catch (Exception e) {
						try {
							resp = testPost(latitude, longitude);
						} catch (Exception e1) {
							try {
								resp = testPost(latitude, longitude);
							} catch (Exception e2) {
								e2.printStackTrace();
								System.out.println(latitude + "," + longitude
										+ " 坐标更新失败");
								continue;
							}
						}
					}
					if (resp.isEmpty()) {
						continue;
					}
					spaceGpsStr = subLalo(resp);
					if (spaceGpsStr == null || spaceGpsStr.isEmpty()
							|| !spaceGpsStr.contains("_")) {
						continue;
					}
					String spaces[] = spaceGpsStr.split("_");
					provinceString = spaces[0];
					cityString = spaces[1];
					districtString = spaces[2];
					street = spaces[3];
					// streetNum = spaces[4];
					cityCode = spaces[5];
					//从百度api获取到的地理位置为空 
					if (provinceString.isEmpty() || provinceString.equals("-")) {
						continue;
					}
					System.out.println(provinceString);

					if (!provinceString.isEmpty()) {
						map.put("province", provinceString);
					}
					if (!districtString.isEmpty()) {
						map.put("district", districtString);
					}
					if (!cityString.isEmpty()) {
						map.put("city", cityString);
					}
					if (!street.isEmpty()) {
						map.put("street", street);
					}
					// if (!streetNum.isEmpty()) {
					// map.put("streetNum", streetNum);
					// }
					if (!cityCode.isEmpty()) {
						map.put("city_code", cityCode);
					}
					//先更新gps表中数据，把相应的地址信息写入
					this.updateTableData(map, tableName, longitude, latitude);
					// 更新区域图数据
					boolean flag1 = this.dealAreaTable(tableName,
							provinceString, cityString, districtString);
					if (flag1) {
							this.updateAreaData(tableName, testCounts, avg_rate,top,down,
									provinceString, cityString, districtString);	
					}
				}
				start += 1000;
				end += 1000;
				close(statement, null);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				close(statement,conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public double getFillcolorred(double speedvalue){
		double fillcolorred = 0;
		if(speedvalue/m_maxspeed < 0.5){
			fillcolorred = 255;
		}else{
			if(speedvalue/m_maxspeed>0){
				fillcolorred = 0;
			}else {
				fillcolorred = (int)(255*(1-speedvalue/m_maxspeed)*2);
			}
		}
		return fillcolorred;
	}
	public double getFillcolorgreen(double speedvalue){
		double fillcolorgreen = 0;
		if (speedvalue / m_maxspeed < 0.5){
			fillcolorgreen = (int)(255 * speedvalue * 2 / m_maxspeed);	
		}
		else{
			fillcolorgreen = 255;	
		}
		return fillcolorgreen;
	}
	public double getfillOpacity(int totalnum){
		double fillOpacity = 0;
		fillOpacity = totalnum / m_maxtestnum;
	    if( fillOpacity < m_opacitymin)
	    	fillOpacity = m_opacitymin;
	    if (fillOpacity > m_opacitymax )
	    	fillOpacity = m_opacitymax;
	    if (fillOpacity == 0)
	    	fillOpacity = 0.001;
	    return fillOpacity;
	}
	/**
	 * 创建区域表并将当前月份的网格化数据表中数据插入区域图
	 * @param table
	 * @param dataOrg
	 * @param tablefrom
	 * @return
	 */
	public boolean createAreaTable(String table,String dataOrg,String tablefrom){
		String numType = "";
		try {
			if(tablefrom.startsWith("speed_test")){
				numType = "01001";
				insertMysqlByDataToGPS.createAreaHTTP(table);
			}else if (tablefrom.startsWith("ping")){
				numType = "04002";
				insertMysqlByDataToGPS.createArea(table);
			}else if(tablefrom.startsWith("http_test") || tablefrom.startsWith("http") ){
				numType = "03001";
				insertMysqlByDataToGPS.createArea(table);
			}
//			this.execute("pro_createtable_area", table);	
			
		} catch (Exception e) {
			System.out.println(table+"表创建失败");
			return false;
		}
		try {
//			this.execute("pro_clearrownum");
			insertMysqlByDataToGPS.proDealAreaData(dataOrg ,numType, table.split("_")[0], table,tablefrom , m_maxspeed, m_opacitymin, m_opacitymax, m_maxtestnum);
//			this.executeProDealData("pro_dealarea_data", dataOrg , table.split("_")[0], table,tablefrom , m_maxspeed, m_opacitymin, m_opacitymax, m_maxtestnum);
		} catch (Exception e) {
			System.out.println(table+"表更新数据失败");
			return false;
		}
		return true;
	}
	
	public boolean dealAreaTable(String tableName,String dbProvince,String dbCity,String dbDistrict){
		System.out.println("处理表:"+tableName+",province："+dbProvince+",city:"+dbCity+",district:"+dbDistrict);
		String [] str = tableName.split("_");
		String data = "";//入库日期
		String provinceTable = "province_"+str[0];
		String cityTable = "city_"+str[0];
		String districtTable = "district_"+str[0];
		if(str.length==4){
			data = str[2];
			provinceTable = "province_"+str[0]+"_"+str[1];
			cityTable="city_"+str[0]+"_"+str[1];
			districtTable= "district_"+str[0]+"_"+str[1];
		}else if(str.length==3){
			data=str[1];
			provinceTable = "province_"+str[0];
			cityTable="city_"+str[0];
			districtTable= "district_"+str[0];
		}
		boolean flag = this.queryTableExist(provinceTable);
		System.out.println(provinceTable+","+cityTable+","+districtTable+",data:"+data+",");
		if(!flag){
			flag = this.createAreaTable(provinceTable,data,tableName);
			if(flag){
				System.out.println(provinceTable+"表创建并更新完成");	
			}
		}else{
			//查询当前月份的数据在表中是否存在
			flag = this.queryTableDataExist(provinceTable,data,dbProvince);
			if(flag){
				//已存在 跳过
			}else{
				//复制上月份数据
				Integer month_num = Integer.parseInt(data.substring(4,6));
				Integer year_num = Integer.parseInt(data.substring(0,4));
				if(month_num==01){
					year_num = year_num - 1;
					month_num = 12;
				}else{
					month_num = month_num - 1;
				}
				String dataOrgUp = "";
				if(month_num.toString().length() == 1){
					 dataOrgUp = year_num+"0"+month_num;
				}else{
					 dataOrgUp = year_num+""+month_num;
				}
				System.out.println("拷贝数据 "+data+", up:"+dataOrgUp+",           table:"+provinceTable);
//				this.execute("pro_copy_areadataup",dataOrgUp, data,provinceTable);
				flag =insertMysqlByDataToGPS.copyAreaDataUp(provinceTable, data, dataOrgUp);
			}
		}
		flag = this.queryTableExist(cityTable);
		if(!flag){
			flag = this.createAreaTable(cityTable,data,tableName);
			if(flag){
				System.out.println(cityTable+"表创建并更新完成");	
			}
		}else{
			//查询当前月份的数据在表中是否存在
			flag = this.queryTableDataExist(cityTable,data,dbCity);
			if(flag){
				//已存在 跳过
			}else{
				//复制上月份数据
				Integer month_num = Integer.parseInt(data.substring(4,6));
				Integer year_num = Integer.parseInt(data.substring(0,4));
				if(month_num==01){
					year_num = year_num - 1;
					month_num = 12;
				}else{
					month_num = month_num - 1;
				}
				String dataOrgUp = "";
				if(month_num.toString().length() == 1){
					 dataOrgUp = year_num+"0"+month_num;
				}else{
					 dataOrgUp = year_num+""+month_num;
				}
				System.out.println("拷贝数据 "+data+", up:"+dataOrgUp+",           table:"+cityTable);
//				this.execute("pro_copy_areadataup",dataOrgUp, data,cityTable);
				flag =insertMysqlByDataToGPS.copyAreaDataUp(cityTable, data, dataOrgUp);
			}
		}
		flag = this.queryTableExist(districtTable);
		if(!flag){
			flag = this.createAreaTable(districtTable,data,tableName);
			if(flag){
				System.out.println(districtTable+"表创建并更新完成");	
			}
		}else{
			//查询当前月份的数据在表中是否存在
			flag = this.queryTableDataExist(districtTable,data,dbDistrict);
			if(flag){
				//已存在 跳过
			}else{
				//复制上月份数据
				Integer month_num = Integer.parseInt(data.substring(4,6));
				Integer year_num = Integer.parseInt(data.substring(0,4));
				if(month_num==01){
					year_num = year_num - 1;
					month_num = 12;
				}else{
					month_num = month_num - 1;
				}
				String dataOrgUp = "";
				if(month_num.toString().length() == 1){
					 dataOrgUp = year_num+"0"+month_num;
				}else{
					 dataOrgUp = year_num+""+month_num;
				}
				System.out.println("拷贝数据 "+data+", up:"+dataOrgUp+",           table:"+districtTable);
//				this.execute("pro_copy_areadataup",dataOrgUp, data,districtTable);
				flag =insertMysqlByDataToGPS.copyAreaDataUp(districtTable, data, dataOrgUp);
			}
		}
		return flag;
	}
	public boolean queryTableDataExist(String tableName,String dataOrg,String area){
		boolean flag = false;
		String sql = "select * from "+tableName+" where data = '"+dataOrg+"'";
		Statement statement = null;
		try {
			statement = getStat();
			ResultSet rs = statement.executeQuery(sql);
			if(rs.next()){
				flag = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			flag = false;
		}finally{
			try {
				close(statement, null);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("查询是否存在："+sql+",是否存在："+flag);
		return flag;
	}
	public boolean queryTableExist(String tableName){
		boolean flag = false;
		DatabaseMetaData databaseMetaData;
		try {
			databaseMetaData = (DatabaseMetaData) conn.getMetaData();
			ResultSet resultSet = databaseMetaData.getTables(null, null, tableName, null);
			if(resultSet.next()){
				flag = true;
			}else{
				flag = false;
			}
			resultSet.close();
		} catch (SQLException e) {
			flag = false;
			e.printStackTrace();
		}finally{
		}
		return flag;
	}
	
	public void updateAreaData(String tableName, int testCounts,
			double avg_rate,double top,double down, String dbProvince, String dbCity, String dbDistrict) {
		boolean flag = false;
		if(tableName.startsWith("speed_test")){
			flag = true;
		}
		Map map = new HashMap();
		Format fm = new DecimalFormat("#.###");
		String [] str = tableName.split("_");
		String data ="";
		String provinceTable = "province_"+str[0];
		String cityTable = "city_"+str[0];
		String districtTable = "district_"+str[0];
		if(str.length==4){
			data = str[2];
			provinceTable = "province_"+str[0]+"_"+str[1];
			cityTable="city_"+str[0]+"_"+str[1];
			districtTable= "district_"+str[0]+"_"+str[1];
		}else if(str.length==3){
			data=str[1];
			provinceTable = "province_"+str[0];
			cityTable="city_"+str[0];
			districtTable= "district_"+str[0];
		}
		String sql1 = "select * from "+provinceTable+" where area = '"+dbProvince+"' ORDER BY id DESC;";
		String sql2 = "select * from "+cityTable+" where area = '"+dbCity+"' ORDER BY id DESC;";
		String sql3 = "select * from "+districtTable+" where area = '"+dbDistrict+"' ORDER BY id DESC;";
		String sql_max1 = "select max(dataNo) max from "+provinceTable;
		String sql_max2 = "select max(dataNo) max from "+cityTable;
		String sql_max3 = "select max(dataNo) max from "+districtTable;
//		String sql_province_dataNo = "select dataNo from "+provinceTable + " where area = '"+dbProvince+"'";
//		String sql_city_dataNo = "select dataNo from "+cityTable + " where area = '"+dbCity+"'";
//		String sql_district_dataNo = "select dataNo from "+districtTable + " where area = '"+dbDistrict+"'";
		String insert_sql = "insert into ";
		String update_sql = "update ";
		String dbData = "";
		Statement statement = null;
		try {
			statement = getStat();
			int max1 = 0;
			ResultSet rs = statement.executeQuery(sql_max1);
			if(rs.next()){
				max1 = rs.getObject("max")==null?0:(rs.getInt("max"));	
			}
			int max2 = 0;
			rs = statement.executeQuery(sql_max2);
			if(rs.next()){
				max2 = rs.getObject("max")==null?0:(rs.getInt("max"));	
			}
			int max3 = 0;
			rs = statement.executeQuery(sql_max3);
			if(rs.next()){
				max3 = rs.getObject("max")==null?0:(rs.getInt("max"));	
			}
//			int provinceDataNo = 0;
//			rs = statement.executeQuery(sql_province_dataNo);
//			if(rs.next()){
//				provinceDataNo=rs.getObject("dataNo")==null?0:rs.getInt("dataNo");
//			}
//			int cityDataNo = 0;
//			rs = statement.executeQuery(sql_city_dataNo);
//			if(rs.next()){
//				cityDataNo=rs.getObject("dataNo")==null?0:rs.getInt("dataNo");
//			}
//			int districtDataNo = 0;
//			rs = statement.executeQuery(sql_district_dataNo);
//			if(rs.next()){
//				districtDataNo=rs.getObject("dataNo")==null?0:rs.getInt("dataNo");
//			}
			
			rs = statement.executeQuery(sql1);
			if(rs.next()){
				dbData = rs.getString("data");
				double avg_rate_db = 0;
				double top_rate_db = 0;
				double down_rate_db = 0;
				if(flag){
					top_rate_db = rs.getDouble("top_rate");
					down_rate_db = rs.getDouble("down_rate");	
				}else{
					avg_rate_db = rs.getDouble("avg_rate");
				}
				int test_counts_db = rs.getInt("test_counts");
				int grid_counts = rs.getInt("grid_counts");
				int provinceDataNo = rs.getInt("dataNo");
				double avg_rate_new = 0;
				double top_rate_new = 0;
				double down_rate_new = 0;
				if((avg_rate != 0 || top!=0 || down!=0)&& testCounts != 0 && test_counts_db != 0){
					if(flag){
						top_rate_new = (top_rate_db*test_counts_db+top*testCounts)/(test_counts_db+testCounts);
						down_rate_new = (down_rate_db*test_counts_db+down*testCounts)/(test_counts_db+testCounts);
						map.put("top_rate", fm.format(top_rate_new));
						map.put("down_rate", fm.format(down_rate_new));
					}else{
						avg_rate_new = (avg_rate_db*test_counts_db+avg_rate*testCounts)/(test_counts_db+testCounts);	
						map.put("avg_rate", fm.format(avg_rate_new));
					}
					
				}
				map.put("data", data);
				map.put("area", dbProvince);
				map.put("test_counts", testCounts+test_counts_db);
				map.put("grid_counts", grid_counts+1);
//				map.put("fillred", this.getFillcolorred(avg_rate_new));
//				map.put("fillgreen", this.getFillcolorgreen(avg_rate_new));
//				map.put("fillOpacity", this.getfillOpacity(grid_counts+1));
				if(dbData.equals(data)){//月份相同 省份相同
					update_sql = this.appendSql(map, provinceTable, "update");
					this.excuteSql(update_sql);
					System.out.println(provinceTable+",sql:"+update_sql);
				}else{//省份相同 月份不同
					map.put("dataNo", provinceDataNo);
					insert_sql = this.appendSql(map, provinceTable, "insert");
					System.out.println(provinceTable+",sql:"+insert_sql);
					this.excuteSql(insert_sql);
				}
			}else{
				map.put("dataNo", max1+1);
				map.put("data", data);
				map.put("area", dbProvince);
				if(flag){
					map.put("top_rate", fm.format(top));
					map.put("down_rate", fm.format(down));
				}else{
					map.put("avg_rate", fm.format(avg_rate));
				}
				map.put("test_counts", testCounts);
				map.put("grid_counts", 1);
//				map.put("fillred", this.getFillcolorred(avg_rate));
//				map.put("fillgreen", this.getFillcolorgreen(avg_rate));
//				map.put("fillOpacity", this.getfillOpacity(1));
				insert_sql = this.appendSql(map, provinceTable, "insert");
				System.out.println(provinceTable+",sql:"+insert_sql);
				this.excuteSql(insert_sql);
			}
			rs = statement.executeQuery(sql2);
			if(rs.next()){
				dbData = rs.getString("data");
				double avg_rate_db = 0;
				double top_rate_db = 0;
				double down_rate_db = 0;
				if(flag){
					top_rate_db = rs.getDouble("top_rate");
					down_rate_db = rs.getDouble("down_rate");	
				}else{
					avg_rate_db = rs.getDouble("avg_rate");
				}
				int test_counts_db = rs.getInt("test_counts");
				int grid_counts = rs.getInt("grid_counts");
				int cityDataNo = rs.getInt("dataNo");
				double avg_rate_new = 0;
				double top_rate_new = 0;
				double down_rate_new = 0;
				if((avg_rate != 0 || top!=0 || down!=0)&& testCounts != 0 && test_counts_db != 0){
					if(flag){
						top_rate_new = (top_rate_db*test_counts_db+top*testCounts)/(test_counts_db+testCounts);
						down_rate_new = (down_rate_db*test_counts_db+down*testCounts)/(test_counts_db+testCounts);
						map.put("top_rate", fm.format(top_rate_new));
						map.put("down_rate", fm.format(down_rate_new));
					}else{
						avg_rate_new = (avg_rate_db*test_counts_db+avg_rate*testCounts)/(test_counts_db+testCounts);	
						map.put("avg_rate", fm.format(avg_rate_new));
					}
					
				}
				map.put("data", data);
				map.put("area", dbCity);
				map.put("test_counts", testCounts+test_counts_db);
				map.put("grid_counts", grid_counts+1);
//				map.put("fillred", this.getFillcolorred(avg_rate_new));
//				map.put("fillgreen", this.getFillcolorgreen(avg_rate_new));
//				map.put("fillOpacity", this.getfillOpacity(grid_counts+1));
				if(dbData.equals(data)){//月份相同 省份相同
					update_sql = this.appendSql(map, cityTable, "update");
					this.excuteSql(update_sql);
					System.out.println(cityTable+",sql:"+update_sql);
				}else{//省份相同 月份不同
					map.put("dataNo", cityDataNo);
					insert_sql = this.appendSql(map, cityTable, "insert");
					System.out.println(cityTable+",sql:"+insert_sql);
					this.excuteSql(insert_sql);
				}
			}else{
				map.put("dataNo", max2+1);
				map.put("data", data);
				map.put("area", dbCity);
				if(flag){
					map.put("top_rate", fm.format(top));
					map.put("down_rate", fm.format(down));
				}else{
					map.put("avg_rate", fm.format(avg_rate));
				}
				map.put("test_counts", testCounts);
				map.put("grid_counts", 1);
//				map.put("fillred", this.getFillcolorred(avg_rate));
//				map.put("fillgreen", this.getFillcolorgreen(avg_rate));
//				map.put("fillOpacity", this.getfillOpacity(1));
				insert_sql = this.appendSql(map, cityTable, "insert");
				System.out.println(cityTable+",sql:"+insert_sql);
				this.excuteSql(insert_sql);
			}
			rs = statement.executeQuery(sql3);
			if(rs.next()){
				dbData = rs.getString("data");
				double avg_rate_db = 0;
				double top_rate_db = 0;
				double down_rate_db = 0;
				if(flag){
					top_rate_db = rs.getDouble("top_rate");
					down_rate_db = rs.getDouble("down_rate");	
				}else{
					avg_rate_db = rs.getDouble("avg_rate");
				}
				int test_counts_db = rs.getInt("test_counts");
				int grid_counts = rs.getInt("grid_counts");
				int districtDataNo = rs.getInt("dataNo");
				double avg_rate_new = 0;
				double top_rate_new = 0;
				double down_rate_new = 0;
				if((avg_rate != 0 || top!=0 || down!=0)&& testCounts != 0 && test_counts_db != 0){
					if(flag){
						top_rate_new = (top_rate_db*test_counts_db+top*testCounts)/(test_counts_db+testCounts);
						down_rate_new = (down_rate_db*test_counts_db+down*testCounts)/(test_counts_db+testCounts);
						map.put("top_rate", fm.format(top_rate_new));
						map.put("down_rate", fm.format(down_rate_new));
					}else{
						avg_rate_new = (avg_rate_db*test_counts_db+avg_rate*testCounts)/(test_counts_db+testCounts);	
						map.put("avg_rate", fm.format(avg_rate_new));
					}
					
				}
				map.put("data", data);
				map.put("area", dbDistrict);
				map.put("test_counts", testCounts+test_counts_db);
				map.put("grid_counts", grid_counts+1);
//				map.put("fillred", this.getFillcolorred(avg_rate_new));
//				map.put("fillgreen", this.getFillcolorgreen(avg_rate_new));
//				map.put("fillOpacity", this.getfillOpacity(grid_counts+1));
				if(dbData.equals(data)){//月份相同 省份相同
					update_sql = this.appendSql(map, districtTable, "update");
					this.excuteSql(update_sql);
					System.out.println(districtTable+",sql:"+update_sql);
				}else{//省份相同 月份不同
					map.put("dataNo", districtDataNo);
					insert_sql = this.appendSql(map, districtTable, "insert");
					System.out.println(districtTable+",sql:"+insert_sql);
					this.excuteSql(insert_sql);
				}
			}else{
				map.put("dataNo", max3+1);
				map.put("data", data);
				map.put("area", dbDistrict);
				if(flag){
					map.put("top_rate", fm.format(top));
					map.put("down_rate", fm.format(down));
				}else{
					map.put("avg_rate", fm.format(avg_rate));
				}
				map.put("test_counts", testCounts);
				map.put("grid_counts", 1);
//				map.put("fillred", this.getFillcolorred(avg_rate));
//				map.put("fillgreen", this.getFillcolorgreen(avg_rate));
//				map.put("fillOpacity", this.getfillOpacity(1));
				insert_sql = this.appendSql(map, districtTable, "insert");
				System.out.println(districtTable+",sql:"+insert_sql);
				this.excuteSql(insert_sql);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				close(statement, null);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public String appendSql(Map map,String table,String type){
		StringBuffer sql = new StringBuffer("");
		if(type.equals("insert")){
			StringBuffer columnStr = new StringBuffer("insert into "+table+" (");
			StringBuffer valueStr = new StringBuffer(" values (");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while(iter.hasNext()){
					String name = (String)iter.next();
					if(name.isEmpty()){
						continue;
					}
					String value = map.get(name)+"";
					if(value.isEmpty()){
						continue;
					}
					columnStr.append(name+",");
					valueStr.append("'"+value+"',");
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
			} catch (Exception e) {
				System.out.println("拼装语句错误："+sql);
				sql = new StringBuffer("");
				e.printStackTrace();
			}finally{
			}
		}else if(type.equals("update")){
			StringBuffer updateStr = new StringBuffer("");
			sql.append("update "+table+" set ");
			try {
				Set set = map.keySet();
				Iterator iter = set.iterator();
				while(iter.hasNext()){
					String name = (String)iter.next();
					if(name.isEmpty()){
						continue;
					}
					String value = map.get(name)+"";
					if(value.isEmpty()){
						continue;
					}
					updateStr.append(name +" = '"+value+"', ");
				}
				while(updateStr.toString().trim().endsWith(",")){
					updateStr = new StringBuffer(updateStr.toString().substring(0,updateStr.toString().lastIndexOf(",")));
				}
				sql.append(updateStr);
				sql.append("where area = '"+map.get("area").toString()+"' and data = '"+map.get("data").toString()+"'");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return sql.toString();
	}
	public String[] getAllTable(String DateString){
		List<String> list = new ArrayList<String>();
		String sql = "select level level from servicequalitymap.map_level";
		Statement statement = null;
		try {
			statement = getStat();
			ResultSet rs = statement.executeQuery(sql);
			while(rs.next()){
				String level = rs.getString("level");
				if(!level.isEmpty() && level.equals("1")){
					for (int i = 0; i < ConfParser.tableType.length; i++) {
						list.add(ConfParser.tableType[i]+"_"+DateString+"_"+level);
					}
					
				}
			}
		} catch (Exception e) {
		}finally{
			try {
				close(statement,null);
			} catch (SQLException e) {
				e.printStackTrace();
			}	
		}
		String []str = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			str[i] = list.get(i).toString();
		}
		return str;
	}
	
	public void updateTableData(Map map,String table,String longitude,String latitude){
		StringBuffer updateStr = new StringBuffer("update "+table+" set " );
		try {
			Set set = map.keySet();
			Iterator iter = set.iterator();
			while (iter.hasNext()) {
				String name = (String) iter.next();
				if (name.isEmpty()) {
					continue;
				}
				String value = map.get(name) + "";
				if (value.isEmpty()) {
					continue;
				}
				updateStr.append(name + " = '" + value + "', ");
			}

			while (updateStr.toString().trim().endsWith(",")) {
				updateStr = new StringBuffer(
						updateStr.toString().substring(0,
								updateStr.toString().lastIndexOf(",")));
			}
			updateStr.append(" where lng = '" + longitude + "' and lat = '" + latitude
							+ "'");
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			this.insert(updateStr.toString());	
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
		}
		
	}
	public Connection getConn() {
		String driver = "com.mysql.jdbc.Driver";
//		String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
		String url = ConfParser.url;
		String user = ConfParser.user;
//		String password = "cmrictpdata";
		String password = ConfParser.password;
		Connection conn = null;
		try {
			if(conn==null){
				Class.forName(driver);
				conn = DriverManager.getConnection(url, user, password);	
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	public Statement getStat() throws SQLException{
		Statement statement = conn.createStatement();
		return statement;
	}

	public void close(Statement statement,Connection conn) throws SQLException {
		if(statement!=null){
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	public boolean insert(String sql) {
		System.out.println("sql:" + sql);
		boolean flag = false;
		Statement statement = null;
		if(sql.isEmpty()){
			flag = false;
		}else{
			try {
				statement = getStat();
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}finally{
				try {
					close(statement, null);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return flag;
	}
	public boolean excuteSql(String sql){
		boolean flag = false;
		Statement statement = null;
		if(!sql.isEmpty()){
			try {
				statement = getStat();
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				flag = false;
			}finally{
				try {
					close(statement, null);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}
	/**
	 * 无参数的存储过程调用
	 * @param name
	 * @param value
	 */
	public void execute(String name) {
        CallableStatement stat = null;
        Connection connection = conn;
        String sql = "{call "+name+" ()}";
        try {
            stat = connection.prepareCall(sql);
            stat.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	try {
				this.close(stat, null);
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
		
	}
	/**
	 * 一个字符串参数的存储过程调用
	 * @param name
	 * @param value
	 */
	public void execute(String name,String value) {
        CallableStatement stat = null;
        Connection connection = conn;
        String sql = "{call "+name+" (?)}";
        try {
            stat = connection.prepareCall(sql);
            stat.setString(1, value);
            stat.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	try {
				this.close(stat, null);
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
		
	}
	/**
	 * 二个字符串参数的存储过程调用
	 * @param name
	 * @param value
	 */
	public void execute(String name,String dataOrgUp,String dataOrg,String tableName) {
        CallableStatement stat = null;
        Connection connection = conn;
        String sql = "{call "+name+" (?,?,?)}";
        try {
            stat = connection.prepareCall(sql);
            stat.setString(1, dataOrgUp);
            stat.setString(2, dataOrg);
            stat.setString(3, tableName);
            stat.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	try {
				this.close(stat, null);
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
	}

	/**
	 * pro_dealarea_data存储过程调用
	 * @param name
	 * @param value
	 */
	public void executeProDealData(String name,String data,String area,String tableto,String tablefrom,double m_maxspeed,double m_opacitymin,double m_opacitymax,double m_maxtestnum) {
        CallableStatement stat = null;
        Connection connection = conn;
        String sql = "{call "+name+" (?,?,?,?,?,?,?,?)}";
        try {
            stat = connection.prepareCall(sql);
            stat.setString(1, data);
            stat.setString(2, area);
            stat.setString(3, tableto);
            stat.setString(4, tablefrom);
            stat.setDouble(5, m_maxspeed);
            stat.setDouble(6, m_opacitymin);
            stat.setDouble(7, m_opacitymax);
            stat.setDouble(8, m_maxtestnum);
            stat.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
        	try {
				this.close(stat, null);
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
        
		
	}
	public static String testPost(String x, String y) throws IOException {

		URL url = new URL("http://api.map.baidu.com/geocoder?ak="+ ConfParser.ak
				+ "&coordtype=wgs84ll&location=" + x + "," + y
				+ "&output=json");
		URLConnection connection = url.openConnection();
		/**
		 * 然后把连接设为输出模式。URLConnection通常作为输入来使用，比如下载一个Web页。
		 * 通过把URLConnection设为输出，你可以把数据向你个Web页传送。下面是如何做：
		 */
		System.out.println(url);
		connection.setDoOutput(true);
		OutputStreamWriter out = new OutputStreamWriter(
				connection.getOutputStream(), "utf-8");
		// remember to clean up
		out.flush();
		out.close();
		// 一旦发送成功，用以下方法就可以得到服务器的回应：
		String res;
		InputStream l_urlStream;
		l_urlStream = connection.getInputStream();
		BufferedReader in = new BufferedReader(new InputStreamReader(
				l_urlStream, "UTF-8"));
		StringBuilder sb = new StringBuilder("");
		while ((res = in.readLine()) != null) {
			sb.append(res.trim());
		}
		String str = sb.toString();
		System.out.println(str);
		return str;

	}

	public static String subLalo(String str) {
		String ssq = "";
		if (StringUtils.isNotEmpty(str)) {
			int cStart = str.indexOf("city\":");
			int cEnd = str.indexOf(",\"direction");
//			int cEnd = str.indexOf(",\"district");

			int dStart = str.indexOf(",\"district");
			int dEnd = str.indexOf(",\"province");

			int pStart = str.indexOf(",\"province");
			int pEnd = str.indexOf(",\"street");

			int sStart = str.indexOf(",\"street");
			int sEnd = str.indexOf(",\"street_number");
			
			int snStart = str.indexOf("street_number");
			
			int ccStart = str.indexOf(",\"cityCode");
			
			if (pStart > 0 && pEnd > 0) {
				String province = str.substring(pStart + 13, pEnd - 1);
				if (StringUtils.isNotBlank(province)) {

					ssq += province + "_";
				} else {
					System.out.println(str+"--->请求百度失败");
					ssq += "-_";
				}
			} else {
				ssq += "-_";
			}
			
			if (cStart > 0 && cEnd > 0) {
				String city = str.substring(cStart + 7, cEnd - 1);
				if (StringUtils.isNotBlank(city)) {
					ssq += city + "_";
				} else {
					ssq += "-_";
				}
			} else {
				ssq += "-_";
			}

			if (dStart > 0 && dEnd > 0) {
				String district = str.substring(dStart + 13, dEnd - 1);
				if (StringUtils.isNotBlank(district)) {

					ssq += district + "_";
				} else {
					ssq += "-_";
				}
			} else {
				ssq += "-" + "_";
			}


			if (sStart > 0 && sEnd > 0) {
				String street = str.substring(sStart + 11, sEnd - 1);
				if (StringUtils.isNotBlank(street)) {

					ssq += street + "_";
				} else {
					ssq += "-_";
				}
			} else {
				ssq += "-_";
			}
			
			if(snStart>0){
				String snStr = str.substring(snStart);
				if(snStr!=null && snStr.length()>0){
					int snEnd = snStart+16;
					String snStr1 = str.substring(snEnd);
					System.out.println(snStr1);
					int snIndex_end = snStr1.indexOf("\"");
					String street_number = snStr1.substring(0,snIndex_end);
					if (StringUtils.isNotBlank(street_number)) {
						ssq += street_number + "_";
					} else {
						ssq += "-_";
					}
				}
			}else
			{
				ssq += "-_";
			}
			
			if(ccStart>0){
				String ccStr = str.substring(ccStart);
				if(ccStr!=null && ccStr.length()>0){
					int ccEnd = ccStart+12;
					String ccStr1 = str.substring(ccEnd);
					System.out.println(ccStr1);
					int ccIndex_end = ccStr1.indexOf("}");
					String city_Code = ccStr1.substring(0,ccIndex_end).trim();
					if (StringUtils.isNotBlank(city_Code)) {
						ssq += city_Code + "_";
					} else {
						ssq += "-_";
					}
				}
			}else
			{
				ssq += "-_";
			}
			
			if(ssq.endsWith("_")){
				ssq = ssq.substring(0, ssq.length()-1);
			}
			return ssq;
		}
		return null;
	}
}
