package com.opencassandra.terminal;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.DataService;
import com.opencassandra.service.impl.DataServiceImpl;
import com.opencassandra.service.utils.MapAddressUtil;
import com.opencassandra.service.utils.UtilDate;
import com.opencassandra.v01.dao.impl.BdFix;
import com.opencassandra.v01.dao.impl.Earth2Mars;

/**
 * 处理终端数据网格化
 * 
 * @author：kxc
 * @date：Nov 19, 2015
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TerminalDao {

	//Statement statement;
	//Connection conn;
	// 数据源数据库的地址
	private static String srcurl = ConfParser.srcurl;
	private static String srcuser = ConfParser.srcuser;
	private static String srcpassword = ConfParser.srcpassword;

	// 数据入库的地址信息
	private static String dsturl = ConfParser.dsturl;
	private static String dstuser = ConfParser.dstuser;
	private static String dstpassword = ConfParser.dstpassword;

	//

	private static String[] testdate = ConfParser.testdate;
	private static String[] mapLevel = ConfParser.mapLevel;
	private static String baiduSwitch = ConfParser.baiduSwitch;
	private static String[] codes = ConfParser.code;
	private static String temptable = ConfParser.temptable;
	private static List levelList = new ArrayList();
	Format fm = new DecimalFormat("#.######");
	// 全局的用于装配以及解析数据使用
	private static Map lnlaMap = new HashMap();

	private static Map<String, Map> gpsMap = new HashMap<String, Map>();
	private static DataService dataServiceImpl = new DataServiceImpl();
	static boolean fag = false;

	// ----------------------------------数据库连接信息-----------------------------------------

	
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
				dataServiceImpl.start(srcurl, srcuser, srcpassword);
			} catch (Exception e) {
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
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return basename;
	}

	// -----------------------------------数据库连接信息-----------------------------------------

	/**
	 * 通过code，时间，表标识查询获取数据源表
	 * 
	 * @param code
	 * @param tdate
	 * @param type
	 * @return
	 * @return String
	 */
	public String getsrctablename(String code, String tdate, String type) {

		ResultSet rs = null;
		if (tdate.isEmpty()) {
			Calendar calendar = Calendar.getInstance();// 获取系统当前时间
			String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
			tdate = month;
		}
		String sql = "select * from date_basename where code = '" + code + " ' and confdate='" + tdate + " ' and datatype='" + type + "' ";
		System.out.println(sql + " ：：：查询对应的数据源表名");
		String srctalename = "";

		try {
			try {
				dataServiceImpl.start(srcurl, srcuser, srcpassword);
			} catch (Exception e) {
				e.printStackTrace();
			}
			rs = (ResultSet) DataServiceImpl.statement.executeQuery(sql);
			if (rs.next()) {
				srctalename = rs.getString("tablename");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return srctalename;
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
			dataServiceImpl.close();
			dataServiceImpl.start(url, dstuser, dstpassword);
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
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 创建入库数据表
	 * 
	 * @param newTableName
	 * @param dataBasename
	 * @param pointer
	 * @return
	 * @return boolean
	 */
	public boolean creatNewTable(String newTableName, String dataBasename) {

		boolean flag = true;
		String sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, imei  varchar(100)  DEFAULT NULL,"
				+ " lng decimal(10,6) DEFAULT NULL, lat decimal(10,6) DEFAULT NULL, testtime varchar(20) DEFAULT NULL,"
				+ " INDEX index_imei (imei), PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dataBasename;
		System.out.println(sql + "::::::建表sql");
		try {
			dataServiceImpl.start(url, dstuser, dstpassword);
			DataServiceImpl.statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	/**
	 * 查询上月份的数据在中间表中是否已经存在
	 * 
	 * @param previousMon
	 * @param dataBaseName
	 * @return
	 * @return boolean
	 */
	public boolean queryMonDataExist(String Mon, String dsBaseName) {
		String sql = "select count(id) count from " + temptable + " where testtime = '" + Mon + "'";
		ResultSet rs = null;
		boolean flage = false;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
			dataServiceImpl.start(url, dstuser, dstpassword);

			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				int count = rs.getInt("count");
				if (count > 0) {
					flage = true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flage;
	}

	/**
	 * 
	 * @param tdate
	 * @param dsBaseName
	 * @return
	 * @return boolean
	 */
	public boolean delToMonData(String tdate, String dsBaseName) {
		String sql = "DELETE FROM " + temptable + " where testtime = '" + tdate + "'";
		boolean flage = false;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
			dataServiceImpl.start(url, dstuser, dstpassword);
			DataServiceImpl.statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flage;
	}

	/**
	 * 查询上月份的数据在中间表中是否已经存在
	 * 
	 * @param previousMon
	 * @param dataBaseName
	 * @return
	 * @return boolean
	 */
	public int queryCount(String dsBaseName) {
		String sql = "select max(id) max  from  " + temptable;
		ResultSet rs = null;
		int count = 0;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
			dataServiceImpl.start(url, dstuser, dstpassword);

			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("max");

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}

	/**
	 * 查询上月份的数据的总值
	 * 
	 * @param previousMon
	 * @param dsBaseName
	 * @return
	 * @return int
	 */
	public int getPreviousMonCount(String previousMon, String dsBaseName) {
		String sql = "select count(id) count from " + temptable + " where testtime=" + previousMon + "";
		ResultSet rs = null;
		int count = 0;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
			dataServiceImpl.start(url, dstuser, dstpassword);

			rs = DataServiceImpl.statement.executeQuery(sql);

			while (rs.next()) {
				count = rs.getInt("count");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}

	/**
	 * 拷贝上月份的数据到临时表中，先把id的起始值更改成count+1的值，再把临时表中的数据的日期更新成tdate月份的时间，
	 * 而后把临时表中的数据复制到gridtable中 最后把临时表删除
	 * 
	 * @param previousMon
	 * @param nextMon
	 * @param dsBaseName
	 * @param count
	 * @return void
	 */
	public void copyAndUpdate(String previousMon, String nextMon, String dsBaseName, int count) {

		try {
			int pre = getPreviousMonCount(previousMon, dsBaseName);

			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
			dataServiceImpl.start(url, dstuser, dstpassword);
			String sql = "DROP TABLE IF EXISTS `tmp`;";
			DataServiceImpl.statement.execute(sql);

			sql = "CREATE TABLE tmp ( id int(10) NOT NULL AUTO_INCREMENT, imei varchar(100)  DEFAULT NULL,"
					+ " lng decimal(10,6) DEFAULT NULL, lat decimal(10,6) DEFAULT NULL, testtime varchar(20) DEFAULT NULL, " + "PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT; ";
			DataServiceImpl.statement.execute(sql);
			sql = "alter table tmp AUTO_INCREMENT = " + (count + 1) + "";
			DataServiceImpl.statement.execute(sql);

			int start = 0;
			int end = 50000;

			boolean copyFla = true;
			while (copyFla) {
				sql = "insert into tmp (imei,lng,lat,testtime) (select imei,lng,lat,testtime from " + temptable + " where testtime = '" + previousMon + "') limit " + start + " ," + end + "";
				DataServiceImpl.statement.execute(sql);
				System.out.println(sql);
				start += 50000;
				if (start > pre) {
					copyFla = false;
				}
			}
			sql = "update tmp set testtime ='" + nextMon + "'";
			DataServiceImpl.statement.execute(sql);

			copyFla = true;
			start = 0;
			while (copyFla) {
				sql = "insert into " + temptable + " select * from tmp limit " + start + " , " + end + "";
				DataServiceImpl.statement.execute(sql);

				start += 50000;
				if (start > pre) {
					copyFla = false;
				}
			}
			sql = "drop table tmp; ";
			DataServiceImpl.statement.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 删除已存在在配置文件中testdate时间中对应的该表
	 * 
	 * @param newTableName
	 * @param srcBaseName
	 * @return void
	 */
	public void delTodMonTable(String newTableName, String srcBaseName) {
		// 应首先查询该数据库是否存在
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		String dropSql = "drop table " + newTableName;
		try {
			dataServiceImpl.start(url, dstuser, dstpassword);
			DataServiceImpl.statement.execute(dropSql);
			dataServiceImpl.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 获取数据源数据
	 * 
	 * @param type
	 * @param tdate
	 * @param srctablename
	 * @param basename
	 * @param pointer
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getDataSrc(String type, String tdate, String srctablename, String basename, int start, int end) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";
		ResultSet rs = null;
		String tomonFirst = "";
		String nextmonFirst = "";
		Calendar calendar = Calendar.getInstance();// 获取系统当前时间
		String todate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
		calendar.add(Calendar.DATE, -1); // 得到前一天
		// String yestedayDate = new
		// SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		// 如果配置中的时间是在本月中，则我们取得数据截止到当天的凌晨
		// 获取的是本月的第一天
		if (!tdate.isEmpty()) {
			// 如果配置中的时间是在本月中，则我们取得数据截止到当天的凌晨
			// 获取的是本月的第一天
			tomonFirst = UtilDate.getFirstDayOfMonth(tdate);
			// 获取下一个月的第一天
			nextmonFirst = UtilDate.getFirstDayOfNextMonth(tdate);
		}
		if (tdate.isEmpty() || tdate.endsWith(month)) {
			sql = "select longitude lng,latitude lat , imei  from " + srctablename + " where longitude > 1  and latitude>1 and  time >=UNIX_TIMESTAMP('" + tomonFirst
					+ "')*1000 and time <UNIX_TIMESTAMP('" + todate + "')*1000  group by imei limit " + start + " , " + end + "";
		} else {
			sql = "select longitude lng,latitude lat ,imei  from " + srctablename + " where longitude > 1 and latitude>1 and  time >=UNIX_TIMESTAMP('" + tomonFirst
					+ "')*1000 and time <UNIX_TIMESTAMP('" + nextmonFirst + "')*1000 group by imei limit " + start + " , " + end + "";
		}
		try {
			dataServiceImpl.close();
			try {
				String url = srcurl.substring(0, srcurl.lastIndexOf("/") + 1) + basename;
				dataServiceImpl.start(url, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(sql + " ::查询ping、http sql");
			rs = DataServiceImpl.statement.executeQuery(sql);

			fag = false;
			while (rs.next()) {
				fag = true;
				Map<String, String> map = new HashMap<String, String>();
				String lng = rs.getString("lng");
				String lat = rs.getString("lat");
				String pointerdata = rs.getString("imei");
				map.put("lng", lng);
				map.put("lat", lat);
				map.put("imei", pointerdata);
				map.put("testtime", tdate);
				listMap.add(map);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return listMap;
	}

	/**
	 * 处理查询出来的数据，若该imei的数据已经存在，则把他的位置信息更新，若不存在则直接插入该记录
	 * 
	 * @param listMap
	 *            数据源数据
	 * @param tdate
	 *            配置的时间
	 * @param dataBaseName
	 *            数据入库的库名
	 * @return List<String>
	 */
	public List<String> dealGridData(List<Map> listMap, String tdate, String dsBaseName) {

		List<String> sqlList = new ArrayList<String>();

		// 遍历listMap数据
		for (Iterator iterator = listMap.iterator(); iterator.hasNext();) {
			Map map = (Map) iterator.next();
			// 该数据为源数据

			String imei = (String) map.get("imei");
			double lng = Double.parseDouble((String) map.get("lng"));
			double lat = Double.parseDouble((String) map.get("lat"));
			String sql = "";
			// 数据是否存在的查询
			boolean flage = this.queryImeiExit(imei, tdate, dsBaseName);
			if (!flage) {
				sql = "insert into " + temptable + " (imei , lng,lat,testtime) values('" + imei + "' , '" + fm.format(lng) + "' ,'" + fm.format(lat) + "' ,'" + tdate + "') ";
			} else {
				sql = "update " + temptable + " set lng='" + fm.format(lng) + "' , lat ='" + fm.format(lat) + "' where imei = '" + imei + "' and testtime='" + tdate + "'";
			}
			System.out.println(sql + "  ::::中间表查询插入sql");
			sqlList.add(sql);
		}
		return sqlList;
	}

	/**
	 * 查询该imei是否已存在
	 * 
	 * @param imei
	 * @param tdate
	 * @param dataBaseName
	 * @return
	 * @return boolean
	 */
	public boolean queryImeiExit(String imei, String tdate, String dataBaseName) {

		String sql = "select id from " + temptable + " where imei = '" + imei + "' and testtime = '" + tdate + "'";

		ResultSet rs = null;

		boolean flage = false;

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dataBaseName;
		try {
			dataServiceImpl.start(url, dstuser, dstpassword);

			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				flage = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return flage;
	}

	// -------------------------------地图等级------------------------------------------

	/**
	 * 根据配置文件中的map等级进行数据库查询对应的lng、lat的值
	 * 
	 * @param mapLevel
	 * @return
	 * @return List
	 */
	public List getLNGLATByLevel(String mapLevel) {
		List list = new ArrayList();
		String sql = "SELECT map.loninterval lng,map.latinterval lat FROM map_level map where level = " + mapLevel;
		double lng = 0;
		double lat = 0;
		ResultSet rs = null;
		System.out.println(sql);
		try {
			dataServiceImpl.close();
			dataServiceImpl.start(dsturl, dstuser, dstpassword);

			rs = DataServiceImpl.statement.executeQuery(sql);
			if (rs.next()) {
				lng = rs.getDouble("lng");
				lat = rs.getDouble("lat");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		list.add(lng);
		list.add(lat);
		return list;
	}

	
	/**
	 * 通过经纬度查询gps_location中该数据是否存在，若存在则直接把地址信息取出
	 * 
	 * @param ln
	 * @param la
	 * @param databasename
	 * @return
	 * @return Map
	 */
	public Map queryDataExit(String ln, String la) {

		String sql = "select * from  gps  where lng='" + ln + "' and lat='" + la + "'";
		Map map = new HashMap();
		ResultSet rs = null;
		boolean fa = false;
		try {
			dataServiceImpl.start(dsturl, dstuser, dstpassword);
			rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				String province = rs.getString("province");
				String city = rs.getString("city");
				String district = rs.getString("district");
				String street = rs.getString("street");
				String cityCode = rs.getString("city_code");
				String formatted_address = rs.getString("formatted_address");
				map.put("province", province);
				map.put("city", city);
				map.put("district", district);
				map.put("street", street);
				map.put("cityCode", cityCode);
				map.put("formatted_address", formatted_address);
				fa = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		map.put("fa", fa);
		return map;
	}

	/**
	 * 插入数据
	 * 
	 * @param sql
	 * @return void
	 */
	public void updateGPSlocation(String sql) {
		try {
			dataServiceImpl.start(dsturl, dstuser, dstpassword);
			dataServiceImpl.insert(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	// -------------------------------处理中间表数据------------------------------------------------

	/**
	 * 查询并处理中间表中的数据，存入终端表中
	 * 
	 * @return
	 * @return boolean
	 */
	public void queryAndDealMiddleData() {

		List lnglatList = new ArrayList();
		System.out.println(gpsMap.size());
		//gpsMap = getGPSLocation();
		System.out.println(gpsMap.size());
		for (int n = 0; n < codes.length; n++) {
			String code = codes[n];
			String dsBaseName = "terminaltest_" + code;

			// 遍历获取到的配置指定时间
			for (int i = 0; i < testdate.length; i++) {
				lnglatList.clear();
				// 配置的指定时间，按该时间进行建表以及数据的查询
				String tdate = testdate[i];

				if (tdate.isEmpty()) {
					Calendar calendar = Calendar.getInstance();// 获取系统当前时间
					String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
					tdate = month;
				}

				int start = 0;
				int end = 50000;
				boolean lagFag = true;
				while (lagFag) {

					// 根据指定时间查询所有临时表中的
					lnglatList = this.getlnglat(temptable, tdate, dsBaseName, start, end);

					if (lnglatList.size() > 0) {

						// 根据时间进行一个数据的查询，如果没有数据的存在则直接跳过
						boolean dateFlage = this.queryIsEx(temptable, tdate, dsBaseName);
						if (dateFlage) {
							for (int j = 0; j < mapLevel.length; j++) {
								String maplevel = mapLevel[j];
								if (!tdate.isEmpty()) {
									String terTable = "terminal" + "_" + tdate + "_" + maplevel;

									try {
										// boolean fal =
										// this.queryTableExit(terTable,
										// dsBaseName);
										if (start == 0) {
											this.createTerTab(terTable, dsBaseName);
										}
									} catch (Exception e) {
										e.printStackTrace();
									}

									levelList = this.getLNGLATByLevel(maplevel);
									if (lnglatList.size() > 0) {
										DealListData(lnglatList, terTable, dsBaseName);
										maptolist(terTable, dsBaseName);
									}
									levelList.clear();
								}
							}
							// this.insertList(terSql, dsBaseName);
						}
					}

					lagFag = fag;
					start += 50000;
				}
			}
		}
	}

	/**
	 * 处理从中间表中查询出来的list数据
	 * 
	 * @param lnglatList
	 * @param terTable
	 * @param dsBaseName
	 * @return void
	 */
	public void DealListData(List lnglatList, String terTable, String dsBaseName) {

		double lng = 0;
		double lat = 0;
		String resp = "";
		String province = null;
		String city = null;
		String district = null;
		String street = null;
		String cityCode = null;
		String formatted_address = null;
		
		boolean queryFlage = false;

		for (Iterator iterator = lnglatList.iterator(); iterator.hasNext();) {
			Map map = (Map) iterator.next();
			lng = (Double) map.get("lng");
			lat = (Double) map.get("lat");

			Map entryMap = new HashMap();

			// 打开关闭百度接口
			if (!baiduSwitch.isEmpty() && "onplat".equals(baiduSwitch)) {
				double gln = (int) (lng / 0.001) * 0.001 + 0.001 / 2;
				double gla = (int) (lat / 0.0008) * 0.0008 + 0.0008 / 2;

				Map lalnMap = new HashMap();
				boolean fa = false;
				if (gpsMap.containsKey(fm.format(gln) + "," + fm.format(gla))) {
					lalnMap = gpsMap.get(fm.format(gln) + "," + fm.format(gla));
					fa = true;
				} else {
					//lalnMap = queryDataExit(fm.format(gln), fm.format(gla));
					fa = false;
							//(Boolean) lalnMap.get("fa");
					queryFlage = true;
				}
				// this.queryDataExit(fm.format(gln), fm.format(gla));
				// boolean fa = (Boolean) lalnMap.get("fa");
				if (!fa) {
					try {
						resp = MapAddressUtil.testPost(String.valueOf(gla), String.valueOf(gln));
					} catch (Exception e) {
						try {
							resp = MapAddressUtil.testPost(String.valueOf(gla), String.valueOf(gln));
						} catch (Exception e1) {
							try {
								resp = MapAddressUtil.testPost(String.valueOf(gla), String.valueOf(gln));
							} catch (Exception e2) {
								e2.printStackTrace();
								System.out.println(gln + "," + gla + " 坐标更新失败");
							}
						}
					}
					if (!resp.isEmpty()) {
						String spaceGpsStr = MapAddressUtil.subLalo(resp);
						if (spaceGpsStr != null && !spaceGpsStr.isEmpty() && spaceGpsStr.contains("_")) {
							String spaces[] = spaceGpsStr.split("_");
							province = spaces[0];
							city = spaces[1];
							district = spaces[2];
							street = spaces[3];
							// streetNum = spaces[4];
							cityCode = spaces[5];
							formatted_address = spaces[6];
							String sql = "insert into gps (lng,lat,province,city,district,street,city_code,formatted_address) " + "values('" + gln + "','" + gla + "'," + "'" + province + "' ,'" + city + "','"
									+ district + "' ,'" + street + "' ,'" + cityCode + "' ,'"+formatted_address+"')";
							// 把最新的地理位置信息更新到常量表GPS_location中
						//	this.updateGPSlocation(sql);

							System.out.println(sql+"         gps位置");
							Map baidumap = new HashMap();
							baidumap.put("province", province);
							baidumap.put("city", city);
							baidumap.put("district", district);
							baidumap.put("street", street);
							baidumap.put("cityCode", cityCode);
							baidumap.put("formatted_address", formatted_address);
							gpsMap.put(fm.format(gln) + "," + fm.format(gla), baidumap);
						}
					}
				} else {
					System.out.println("  :::该经纬度的数据在GPS_location中已存在：：：");
					province = (String) lalnMap.get("province");
					city = (String) lalnMap.get("city");
					district = (String) lalnMap.get("district");
					street = (String) lalnMap.get("street");
					cityCode = (String) lalnMap.get("cityCode");
					formatted_address = (String) lalnMap.get("formatted_address");
					if (queryFlage) {
						gpsMap.put(fm.format(gln) + "," + fm.format(gla), lalnMap);
					}
				}
			}

			entryMap.put("province", province);
			entryMap.put("city", city);
			entryMap.put("district", district);
			entryMap.put("street", street);
			entryMap.put("cityCode", cityCode);
			entryMap.put("formatted_address", formatted_address);

			// 处理经纬度数据
			double[] em = Earth2Mars.transform(lng, lat);
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			double longitudeNum = bf[0];
			double latitudeNum = bf[1];

			double LNGinterval = (Double) levelList.get(0);
			double LATinterval = (Double) levelList.get(1);
			lng = ((int) (longitudeNum / LNGinterval)) * LNGinterval + LNGinterval / 2;
			lat = ((int) (latitudeNum / LATinterval)) * LATinterval + LATinterval / 2;

			String key = fm.format(lng) + "," + fm.format(lat);
			if (lnlaMap.containsKey(key)) {
				Map value = (Map) lnlaMap.get(key);
				String val = (String) value.get(key);
				int testtimes = Integer.parseInt(val);
				// 已存在全局map中的值
				testtimes = testtimes + 1;

				entryMap.put(fm.format(lng) + "," + fm.format(lat), testtimes + "");
			} else {
				entryMap.put(fm.format(lng) + "," + fm.format(lat), "1");
			}
			lnlaMap.put(fm.format(lng) + "," + fm.format(lat), entryMap);

			if (lnlaMap.size() % 10000 == 0) {
				maptolist(terTable, dsBaseName);
			}
		}
	}

	/**
	 * 对数据进行查重并进行数据插入
	 * 
	 * @param therTable
	 * @return void
	 */
	public void maptolist(String newTableName, String databasename) {
		String sql = "";
		double ln = 0;
		double la = 0;
		List<String> sqlList = new ArrayList<String>();
		// 对全局的lnlamap进行遍历 lnlaMap的结构是 lnla为key val为一个map ，里面map的结构是
		// lnla为key的一个值是avg、times的值，其他的是地址信息
		Iterator iter = lnlaMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			// 该key即为lnla经纬度信息
			String lnglat = entry.getKey().toString();
			// 获取子map
			Map map = (Map) entry.getValue();
			// 该数据即为avg、time的信息
			String avgTime = (String) map.get(lnglat);
			ln = Double.parseDouble(lnglat.substring(0, lnglat.indexOf(",")));
			la = Double.parseDouble(lnglat.substring(lnglat.indexOf(",") + 1, lnglat.length()));
			// 该数据为map中的数据
			int times_map = Integer.parseInt(avgTime);

			// 根据经纬度查询该数据是否已存在
			Map nummap = this.queryTer(newTableName, fm.format(ln), fm.format(la), databasename);
			boolean flage = (Boolean) nummap.get("flag");
			// 不存在执行插入
			String province = (String) map.get("province");
			String city = (String) map.get("city");
			String district = (String) map.get("district");
			String street = (String) map.get("street");
			String cityCode = (String) map.get("cityCode");
			String formatted_address = (String) map.get("formatted_address");
			if (!flage) {
				if (!baiduSwitch.isEmpty() && "onplat".equals(baiduSwitch)) {
					sql = "insert into " + newTableName + " (lng,lat,terminalnum, province,city,district,street,city_code,formatted_address,street_number) " + "values('" + fm.format(ln) + "','" + fm.format(la) + "','"
							+ times_map + "','" + province + "','" + city + "','" + district + "','" + street + "','" + cityCode + "' ,'"+formatted_address+"' ,null)";
				} else {
					sql = "insert into " + newTableName + " (lng,lat,terminalnum,province,city,district,street,city_code,formatted_address ,street_number) " + "values(" + "'" + fm.format(ln) + "','" + fm.format(la)
							+ "','" + times_map + "'," + "" + province + "," + city + "," + district + "," + street + "," + cityCode + " ,"+formatted_address+ ", null )";
				}

			} else {
				// 该数据为已存入数据库的数据
				int times_base = (Integer) nummap.get("terminalnum");
				times_base = times_base + times_map;
				sql = "update " + newTableName + " set terminalnum='" + times_base + "'  where id=" + nummap.get("id");
			}
			// System.out.println(sql);
			sqlList.add(sql);
		}
		// 插入数据
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + databasename;
		try {
			dataServiceImpl.start(url, dstuser, dstpassword);
		}catch(Exception e){
			e.printStackTrace();
		}
		dataServiceImpl.insertList(sqlList, databasename);
		// 清空list
		sqlList.clear();
		// 清空map
		lnlaMap.clear();
	}

	/**
	 * 按月份获取到临时表中的终端数据，并把数据放入到listmap中
	 * 
	 * @param table
	 * @param testdate
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getlnglat(String table, String testdate, String dsBaseName, int start, int end) {
		List<Map> list = new ArrayList<Map>();
		String sql = "select grid.lng lng,grid.lat lat FROM " + table + " grid where testtime ='" + testdate + "'  limit " + start + " , " + end + "";
		double lng = 0;
		double lat = 0;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
		try {
			dataServiceImpl.start(url, dstuser, dstpassword);
			System.out.println(sql);
			ResultSet rs = DataServiceImpl.statement.executeQuery(sql);
			fag = false;
			while (rs.next()) {
				fag = true;
				Map map = new HashMap();
				lng = rs.getDouble("lng");
				lat = rs.getDouble("lat");
				map.put("lng", lng);
				map.put("lat", lat);
				list.add(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	/**
	 * 根据测试时间进行数据的查询，看着一个月份的数据是否存在
	 * 
	 * @param date
	 * @return
	 * @return boolean
	 */
	public boolean queryIsEx(String temptable, String date, String dsBaseName) {
		String sql = "select count(id) cou FROM " + temptable + " grid where grid.testtime ='" + date + "'";
		boolean flage = false;
		int count = 0;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
		try {
			dataServiceImpl.start(url, dstuser, dstpassword);
			ResultSet rs = DataServiceImpl.statement.executeQuery(sql);
			while (rs.next()) {
				count = rs.getInt("cou");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (count > 0) {
			flage = true;
		}
		System.out.println(flage + "   ::::::按月份数据查询的结果");
		return flage;
	}

	/**
	 * 创建正式表
	 * 
	 * @param tablename
	 * @return
	 * @return boolean
	 */
	public boolean createTerTab(String tablename, String dsBaseName) {
		boolean flag = true;
		String dropSql = "DROP TABLE IF EXISTS `" + tablename + "`;";//formatted_address
		String sql = "CREATE TABLE " + tablename + " ( id int(10) NOT NULL AUTO_INCREMENT,lng decimal(10,6) DEFAULT NULL,lat decimal(10,6) DEFAULT NULL,"
				+ "terminalnum int(10) DEFAULT NULL,`formatted_address`  varchar(200) DEFAULT NULL , `province`  varchar(200) DEFAULT NULL ,`city`  varchar(200) DEFAULT NULL ,"
				+ "`district`  varchar(200)  DEFAULT NULL ,`street`  varchar(200) DEFAULT NULL ,`street_number`  varchar(200) DEFAULT NULL ,"
				+ "`city_code`  varchar(200) DEFAULT NULL , index index_lnglat (lng,lat), PRIMARY KEY (id)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
		try {
			dataServiceImpl.start(url, dstuser, dstpassword);
			DataServiceImpl.statement.execute(dropSql);
			DataServiceImpl.statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;

	}

	/**
	 * 查询ter表中是否存在
	 * 
	 * @param table
	 * @param lng
	 * @param lat
	 * @return
	 * @return boolean
	 */
	public Map queryTer(String table, String lng, String lat, String dsBaseName) {

		String sql = "select * from " + table + " where lng = '" + lng + "' and lat = " + lat;
		Map map = new HashMap();
		System.out.println(sql + "   :::::::查询数据存在sql");
		boolean flage = false;
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dsBaseName;
		try {
			try {
				dataServiceImpl.start(url, dstuser, dstpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ResultSet rs = (ResultSet) DataServiceImpl.statement.executeQuery(sql);
			if (rs.next()) {
				flage = true;
				map.put("id", rs.getString("id"));
				map.put("terminalnum", rs.getInt("terminalnum"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		map.put("flag", flage);
		return map;
	}

	/**
	 * 查询长量表中地理位置信息
	 * 
	 * @return
	 * @return Map<String,Map>
	 */
	public Map<String, Map> getGPSLocation() {
		Map<String, Map> gpsMap = new HashMap<String, Map>();
		String sql = "select *  from gps ";
		ResultSet rs = null;
		try {
			dataServiceImpl.start(dsturl, dstuser, dstpassword);
			rs = DataServiceImpl.statement.executeQuery(sql);

			while (rs.next()) {
				Map map = new HashMap();
				String lng = rs.getDouble("lng") + "";
				String lat = rs.getDouble("lat") + "";
				String province = rs.getString("province");
				String city = rs.getString("city");
				String district = rs.getString("district");
				String street = rs.getString("street");
				String city_code = rs.getString("city_code");
				String formatted_address = rs.getString("formatted_address");
				map.put("province", province);
				map.put("city", city);
				map.put("district", district);
				map.put("street", street);
				map.put("cityCode", city_code);
				map.put("formatted_address", formatted_address);

				gpsMap.put(lng + "," + lat, map);
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			try {
				dataServiceImpl.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return gpsMap;
	}

	public static void main(String[] args) {
		TerminalDao dao = new TerminalDao();
		dao.queryAndDealMiddleData();
	}

}
