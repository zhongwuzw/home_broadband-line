package com.opencassandra.areachart;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.DataService;
import com.opencassandra.service.impl.DataServiceImpl;

/**
 * 处理一定级别的网格化数据区域图
 * 
 * @author：kxc
 * @date：Nov 26, 2015
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class AreaChartDao {

	// ----------------------------数据源ip地址-----------------------------------
	private static String srcurl = ConfParser.srcurl;
	private static String srcuser = ConfParser.srcuser;
	private static String srcpassword = ConfParser.srcpassword;

	// ----------------------------数据入库ip地址---------------------------------
	private static String dsturl = ConfParser.dsturl;
	private static String dstuser = ConfParser.dstuser;
	private static String dstpassword = ConfParser.dstpassword;

	private static DataService dataService = new DataServiceImpl();

	// ----------------------------------数据库连接信息-----------------------------------------

	/**
	 * 连接数据库，若不存在则创建该数据库
	 * 
	 * @param code
	 * @param srcBaseName
	 * @return void
	 */
	public void startOrCreateDB(String code, String srcBaseName) {

		// 应首先查询该数据库是否存在
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		try {
			dataService.start(url, dstuser, dstpassword);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// 创建相应的数据库
			dataService.createDB(srcBaseName);
			try {
				dataService.start(url, dstuser, dstpassword);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 根据code从code_basename对应表中获取到对应的数据库名称
	 * 
	 * @param code
	 * @return
	 * @return String
	 */
	public String getDatabaseName(String code) {

		String sql = "select * from code_basename where confcode = '" + code + "'";
		System.out.println(sql + " :::根据code查询数据库名");
		String basename = "";
		ResultSet rs = null;
		try {
			try {
				dataService.start(srcurl, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			rs = (ResultSet) DataServiceImpl.statement.executeQuery(sql);
			if (rs.next()) {
				basename = rs.getString("basename");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			try {
				dataService.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return basename;
	}

	/**
	 * 查询数据库中表是否存在
	 * 
	 * @param tableName
	 * @return
	 * @return boolean
	 */
	public boolean queryTableExit(String newtableName, String dataBasename) {

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dataBasename;
		try {
			dataService.close();
			dataService.start(url, dstuser, dstpassword);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean flag = false;
		ResultSet resultSet = null;
		DatabaseMetaData databaseMetaData;
		try {
			databaseMetaData = (DatabaseMetaData) DataServiceImpl.conn.getMetaData();
			resultSet = databaseMetaData.getTables(null, null, newtableName, null);
			if (resultSet.next()) {
				flag = true;
			} else {
				flag = false;
			}
			resultSet.close();
		} catch (SQLException e) {
			flag = false;
			e.printStackTrace();
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				dataService.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 创建区域图表
	 * 
	 * @param newTableName
	 * @param dataBasename
	 * @param pointer
	 * @return
	 * @return boolean
	 */
	public boolean creatNewTable(String newTableName, String dataBasename, String tabPointer) {

		boolean flag = true;
		String sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT,`dataNo`  int(11) NOT NULL ," +
				"`data`  varchar(50) DEFAULT NULL ,`area`  varchar(200) DEFAULT NULL ,"
				+ "`test_counts`  int(10) DEFAULT NULL ,`grid_counts`  int(10) DEFAULT NULL ," + "`" + tabPointer
				+ "`  decimal(10,3) DEFAULT NULL ,PRIMARY KEY (`id`)) CHARSET=utf8 ROW_FORMAT=COMPACT;";

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dataBasename;
		System.out.println(sql + "::::::建表sql");
		try {
			dataService.start(url, dstuser, dstpassword);
			DataServiceImpl.statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 删除该月的数据
	 * 
	 * @param newTableName
	 * @param srcBaseName
	 * @param tdate
	 * @return void
	 */
	public void delTodMonData(String newTableName, String srcBaseName, String tdate) {

		String sql = "delete from " + newTableName + " where data='" + tdate + "'";
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		System.out.println(sql + "  ::::");
		try {
			dataService.start(url, dstuser, dstpassword);
			DataServiceImpl.statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * sql查询出所需的源数据
	 * 
	 * @param srcTableName
	 * @param srcBaseName
	 * @param pointer
	 * @return
	 * @return List<Map<String,String>>
	 */

	public List<Map> getDataList(String srcTableName, String srcBaseName, String pointer, String tabPointer) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";
		ResultSet resultSet = null;
		
		if(pointer.equals("district")){
			if (srcTableName.startsWith("ping")) {
				sql = "select " + pointer + "  ,SUM(test_times) as test_counts ,count(id) grid_counts , sum(test_times)/sum(test_times/" + tabPointer + ") as avg_rate " +
						" from " + srcTableName + " where LENGTH("+pointer+")>1  group by " + pointer + ",province,city order by avg_rate desc";
			} else {
				sql = "select " + pointer + " , sum(test_times) as test_counts ,count(id) grid_counts ,sum(" + tabPointer + " *test_times)/sum(test_times) as avg_rate " +
						" from " + srcTableName + " where LENGTH("+pointer+")>1  group by " + pointer + " ,province,city order by avg_rate desc";
			}

		}else{
			if (srcTableName.startsWith("ping")) {
				sql = "select " + pointer + "  ,SUM(test_times) as test_counts ,count(id) grid_counts , sum(test_times)/sum(test_times/" + tabPointer + ") as avg_rate " +
						" from " + srcTableName + " where LENGTH("+pointer+")>1  group by " + pointer + " order by avg_rate desc";
			} else {
				sql = "select " + pointer + " , sum(test_times) as test_counts ,count(id) grid_counts ,sum(" + tabPointer + " *test_times)/sum(test_times) as avg_rate " +
						" from " + srcTableName + " where LENGTH("+pointer+")>1  group by " + pointer + " order by avg_rate desc";
			}

		}
		
		
		System.out.println(sql + " ;::::::");
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		try {
			dataService.start(url, dstuser, dstpassword);
			resultSet = DataServiceImpl.statement.executeQuery(sql);
			while (resultSet.next()) {
				Map map = new HashMap();
				String area = resultSet.getString(pointer);
				int test_counts = resultSet.getInt("test_counts");
				int grid_counts = resultSet.getInt("grid_counts");
				double avg = resultSet.getDouble("avg_rate");

				map.put("area", area);
				map.put("test_counts", test_counts);
				map.put("grid_counts", grid_counts);
				map.put("avg", avg);

				listMap.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return listMap;
	}

	/**
	 * 处理i以查询出的源数据
	 * 
	 * @param dataList
	 * @param srcTableName
	 * @param newTableName
	 * @param srcBaseName
	 * @return void
	 */
	public void dealData(List dataList, String newTableName, String srcBaseName, String tabPointer, String tdate) {

		String sql = "";
		AreaChartDao areaChartDao = new AreaChartDao();
		List<String> sqlList = new ArrayList<String>();

		// 获取最大的dataNo值
		int maxDataNo = areaChartDao.queryMaxNo(newTableName, srcBaseName);

		for (Iterator iterator = dataList.iterator(); iterator.hasNext();) {
			Map map = (Map) iterator.next();

			maxDataNo++;
			String area = (String) map.get("area");
			int test_counts = (Integer) map.get("test_counts");
			int grid_counts = (Integer) map.get("grid_counts");
			Double avg = (Double) map.get("avg");
			// 查询该数据是否已存在
			Map queryMap = areaChartDao.queryDataExist(srcBaseName, newTableName, area, tdate, tabPointer);

			// 该flage用于判断该地区在该月份是否已存在
			boolean flage = (Boolean) queryMap.get("flage");
			String id = (String) queryMap.get("id");
			if (flage) {
				// 该值为表中已存在的test_counts
				int queryTest = (Integer) queryMap.get("test_counts");
				// 该值为表中已存在的grid_counts
				int queryGrid = (Integer) queryMap.get("grid_counts");
				// 该值为表中已存在的avg值
				double queryAvg = (Double) queryMap.get("avg");

				int testcount = test_counts + queryTest;
				int gridcount = grid_counts + queryGrid;

				double avgs = 0;
				// ping表的tabPointer值为时延avg_rtt，其算法为(times1+ times2)/(times1/rtt1
				// + times2/rtt2)
				// http、speed表的为平均速率avg_rate，其算法为(times1*rate1+time2*rate2)/(time1+time2)是为平均值
				if (newTableName.contains("ping")) {
					if (queryAvg == 0) {
						avgs = avg;
					} else {
						avgs = (testcount) / ((test_counts) / avg + queryTest / queryAvg);
					}
				} else {
					avgs = (queryTest * queryAvg + test_counts * avg) / testcount;
				}

				sql = "update " + newTableName + " set test_counts=" + testcount + " ,grid_counts=" + gridcount + " , " + tabPointer + "=" + avgs + "  where id=" + id + "";
			} else {
				sql = "insert into " + newTableName + " (dataNo , data ,area,test_counts,grid_counts," + tabPointer + ") " + "values ('" + maxDataNo + "','" + tdate + "' ,'" + area + "', '"
						+ test_counts + "' ,'" + grid_counts + "' ,'" + avg + "' )";
			}
			sqlList.add(sql);
		}

		// 批量执行sql
		dataService.insertList(sqlList, srcBaseName);
	}

	/**
	 * 查询最大的dataNo值，用于后面使用时保证dataNo为顺序递增
	 * 
	 * @param newTableName
	 * @param srcBaseName
	 * @return
	 * @return int
	 */
	private int queryMaxNo(String newTableName, String srcBaseName) {
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		String sql = "select max(dataNo) max from " + newTableName + "";
		int max = 0;
		ResultSet rs = null;
		try {
			dataService.start(url, dstuser, dstpassword);
			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				max = rs.getInt("max");
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return max;
	}

	/**
	 * 查询该区域数据是否已存在
	 * 
	 * @param srcBaseName
	 * @param newTableName
	 * @param area
	 * @param tdate
	 * @param tabPointer
	 * @return
	 * @return Map
	 */
	private Map queryDataExist(String srcBaseName, String newTableName, String area, String tdate, String tabPointer) {
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		String sql = "select * from " + newTableName + " where area='" + area + "' and data = " + tdate + "";
		ResultSet rs = null;
		boolean flage = false;
		Map map = new HashMap();
		try {
			dataService.start(url, dstuser, dstpassword);
			System.out.println(sql + "  根据时间，地址查询");
			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {

				int grid_counts = rs.getInt("grid_counts");
				double avg = rs.getDouble(tabPointer);
				int test_counts = rs.getInt("test_counts");
				String id = rs.getString("id");

				flage = true;
				map.put("id", id);
				map.put("grid_counts", grid_counts);
				map.put("avg", avg);
				map.put("test_counts", test_counts);

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		map.put("flage", flage);
		return map;
	}

	/**
	 * 查询上月份的数据是否存在
	 * 
	 * @param newTableName
	 * @param srcBaseName
	 * @param preMon
	 * @return
	 * @return boolean
	 */
	public boolean queryPreMon(String newTableName, String srcBaseName, String preMon) {

		String sql = "select count(id) count from " + newTableName + " where data='" + preMon + "'";
		int count = 0;
		ResultSet rs = null;
		boolean flage = false;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		try {
			dataService.start(url, dstuser, dstpassword);

			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("count");
				if (count > 0) {
					flage = true;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flage;
	}

	/**
	 * 利用中间临时表复制上月份的数据（主要是处理dataNo的值在相同的省市区下是相同的）
	 * 
	 * @param newTableName
	 * @param srcBaseName
	 * @param tabPointer
	 * @param tdate
	 * @param preMon
	 * @param maxId
	 * @return void
	 */
	public void copyPreToMon(String newTableName, String srcBaseName, String tabPointer, String tdate, String preMon, int maxId) {

		try {

			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
			dataService.start(url, dstuser, dstpassword);
			String sql = "DROP TABLE IF EXISTS `tmp`;";
			DataServiceImpl.statement.execute(sql);

			sql = "CREATE TABLE tmp ( id int(10) NOT NULL AUTO_INCREMENT,`dataNo`  int(11) NOT NULL ," +
					"`data`  varchar(50) DEFAULT NULL ,`area`  varchar(200) DEFAULT NULL ,"
					+ "PRIMARY KEY (`id`)) CHARSET=utf8 ROW_FORMAT=COMPACT;";

			DataServiceImpl.statement.execute(sql);
			sql = "alter table tmp AUTO_INCREMENT = " + (maxId + 1) + "";
			DataServiceImpl.statement.execute(sql);

			sql = "insert into tmp (dataNo,data,area) (select dataNo,data,area  from " + newTableName + " where data = '" + preMon + "') ";
			DataServiceImpl.statement.execute(sql);
			sql = "update tmp set data ='" + tdate + "'";
			DataServiceImpl.statement.execute(sql);

			sql = "insert into " + newTableName + "(id,dataNo,data,area) select * from tmp ";
			DataServiceImpl.statement.execute(sql);
			sql = "drop table tmp; ";
			DataServiceImpl.statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 查询最大的id值，用于表内复制增量的初始值
	 * 
	 * @param newTableName
	 * @param srcBaseName
	 * @return
	 * @return int
	 */
	public int queryMaxId(String newTableName, String srcBaseName) {
		String sql = "select max(id) max from " + newTableName + "";
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		int count = 0;
		ResultSet rs = null;
		try {
			dataService.start(url, dstuser, dstpassword);

			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("max");
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataService.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}

}
