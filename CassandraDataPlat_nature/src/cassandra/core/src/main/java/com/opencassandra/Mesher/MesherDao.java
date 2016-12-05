package com.opencassandra.Mesher;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mysql.jdbc.DatabaseMetaData;
import com.opencassandra.Mesher.database.DatabaseUtil;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.utils.UtilDate;

/**
 * 网格化数据通用
 * 
 * @author：kxc
 * @date：Nov 9, 2015
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MesherDao {
	Statement statement;
	Connection conn;
	// 数据源数据库的地址
	private static String srcurl = ConfParser.srcurl;
	private static String srcuser = ConfParser.srcuser;
	private static String srcpassword = ConfParser.srcpassword;

	// 数据入库的地址信息
	public static String dsturl = ConfParser.dsturl;
	public static String dstuser = ConfParser.dstuser;
	public static String dstpassword = ConfParser.dstpassword;

	static boolean fals = false;
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
				start(srcurl, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			rs = (ResultSet) statement.executeQuery(sql);
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
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return basename;
	}

	// -----------------------------------------------------数据库连接相关操作----------------------------------------------------------
	/**
	 * 建立数据库连接
	 * 
	 * @return void
	 */
	public void start(String url, String user, String password) throws Exception {
		System.out.println(url + " :数据库连接: " + user + " :: " + password);
		this.close();
		String driver = "com.mysql.jdbc.Driver";
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
	}

	/**
	 * 关闭数据库连接
	 * 
	 * @throws SQLException
	 * @return void
	 */
	public void close() throws SQLException {
		if (statement != null) {
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}

	/**
	 * 创建数据库
	 * 
	 * @param database
	 * @return void
	 */
	public void createDB(String database) {
		try {
			// ConfParser.dsturl = ConfParser.dsturl.substring(0,
			// ConfParser.dsturl.lastIndexOf("/"));
			start(dsturl, dstuser, dstpassword);
			String sql = "create DATABASE " + database;
			statement.execute(sql);
			close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 执行单条插入、更新语句
	 * 
	 * @param sql
	 * @return
	 * @return boolean
	 */
	public boolean insert(String sql) {

		System.out.println("sql:" + sql);
		boolean flag = false;
		if (sql.isEmpty()) {
			flag = false;
		} else {
			try {
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}
		}
		return flag;
	}

	/**
	 * 插入数据
	 * 
	 * @param sql
	 * @return void
	 */
	public void updateGPSlocation(String sql) {
		try {
			this.start(dsturl, dstuser, dstpassword);
			this.insert(sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * 连接数据库，若不存在则创建该数据库
	 * @param code
	 * @param srcBaseName
	 * @return void
	 */
	public void startOrCreateDB(String srcBaseName) {

		// 应首先查询该数据库是否存在
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		try {
			start(url, dstuser, dstpassword);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// 创建相应的数据库
			createDB(srcBaseName);
			try {
				start(url, dstuser, dstpassword);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			try {
				this.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 删除已存在在配置文件中testdate时间中对应的该表
	 * @param newTableName
	 * @param srcBaseName
	 * @return void
	 */
	public void delTodMonTable(String newTableName, String srcBaseName) {
		// 应首先查询该数据库是否存在
		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + srcBaseName;
		String dropSql = "drop table " + newTableName;
		try {
			start(url, dstuser, dstpassword);
			statement.execute(dropSql);
			close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// -------------------------------------------------------------------------------------------------------------------

	/**
	 * 通过code，时间，表标识查询获取数据源表
	 * 
	 * @param code
	 * @param tdate
	 * @param type
	 * @return
	 * @return String
	 */
	public String getsrctablename(String code, String tdate, String type,String basename) {

		ResultSet rs = null;
		if (tdate.isEmpty()) {
			Calendar calendar = Calendar.getInstance();// 获取系统当前时间
			String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
			tdate = month;
		}
		
		
		if(type.contains("ping")||type.contains("signalstrength")){
			type= "ping";
		}else if(type.startsWith("speed_test")){
			type = "speed_test";
		}else if(type.startsWith("http")){
			type = "http_test";
		}
		
		String sql = "select newtablename from date_tablename where  confdate='" + tdate + " ' and datatype='" + type + "' ";
		
		//String sql = "select * from date_basename where code = '" + code + " ' and confdate='" + tdate + " ' and datatype='" + type + "' ";
		System.out.println(sql + " ：：：查询对应的数据源表名");
		String srctalename = "";

		try {
			try {
				String url = srcurl.substring(0, srcurl.lastIndexOf("/") + 1) + basename;
				this.start(url, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			rs = (ResultSet) statement.executeQuery(sql);
			if (rs.next()) {
				srctalename = rs.getString("newtablename");
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
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return srctalename;
	}

	public List<Map> getDataSrc(String type, String tdate, String srctablename, String basename, String pointer,int start,int end) {

		List<Map> dataListMap = new ArrayList<Map>();
		// 第一个判断是先把信号强度的类型过滤出来，再在信号强度内部进行2、3、4G的划分
		if (type.toUpperCase().startsWith("2G") || type.toUpperCase().startsWith("3G") || type.toUpperCase().startsWith("4G")|| type.toUpperCase().startsWith("WIFI")) {
			dataListMap = this.getTherData(type, tdate, srctablename, basename, pointer,start,end);
		} else if (type.startsWith("ping_2G") || type.startsWith("ping_3G") || type.startsWith("ping_4G")||type.startsWith("ping_WIFI")
					||type.startsWith("http_2G")||type.startsWith("http_3G")||type.startsWith("http_4G")||type.startsWith("http_WIFI")
					||type.startsWith("speed_test_up_2G")||type.startsWith("speed_test_up_3G")||type.startsWith("speed_test_up_4G")||type.startsWith("speed_test_up_WIFI")
					||type.startsWith("speed_test_down_2G")||type.startsWith("speed_test_down_3G")||type.startsWith("speed_test_down_4G")||type.startsWith("speed_test_down_WIFI")
				) {
			dataListMap = this.getPingGDate(type, tdate, srctablename, basename, pointer,start,end);
		} else {
			dataListMap = this.getOtherData(type, tdate, basename, srctablename, pointer,start,end);
		}

		return dataListMap;
	}

	/**
	 * 根据type、tdate、数据源表名、数据库名查询数据
	 * 
	 * @param type
	 * @param tdate
	 * @param srctablename
	 * @param basename
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getTherData(String type, String tdate, String srctablename, String basename, String pointer,int start,int end) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";
		String tomonFirst = "";
		String nextmonFirst = "";
		ResultSet rs = null;
		Calendar calendar = Calendar.getInstance();// 获取系统当前时间
		String todate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
		calendar.add(Calendar.DATE, -1); // 得到前一天
		String yestedayDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		if (!tdate.isEmpty()) {
			// 如果配置中的时间是在本月中，则我们取得数据截止到当天的凌晨
			// 获取的是本月的第一天
			tomonFirst = UtilDate.getFirstDayOfMonth(tdate);
			// 获取下一个月的第一天
			nextmonFirst = UtilDate.getFirstDayOfNextMonth(tdate);
		}
		// 如果是没有配置月份，则该程序执行的是前一天的数据
		if (tdate.isEmpty()) {
			if (type.toUpperCase().startsWith("2G")) {
				sql = "select longitude lng,latitude lat  , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " where longitude > 1 and signal_strength < 0 and "
						+ "(net_type like '%EDGE%' or net_type like '%GPRS%' or net_type like '%GSM%') " + "   limit  "+start+" ,  "+end+"";
			} else if (type.toUpperCase().startsWith("3G")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " " + "where longitude > 1 and signal_strength < 0 and net_type like '%TD-S%' "
						+ "   limit  "+start+" ,  "+end+"";
			} else if (type.toUpperCase().startsWith("4G")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and signal_strength < 0 and net_type like '%LTE%' "
						+ "   limit  "+start+" ,  "+end+"";
			}else if (type.toUpperCase().startsWith("WIFI")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and signal_strength < 0 and net_type like '%WIFI%' "
						+ "   limit  "+start+" ,  "+end+"";
			}
			// 如果配置的时间是当月的时间，则数据是从月初的第一天到当前时间的凌晨，也就是到前一天的数据
		} else if (tdate.equals(month)) {
			if (type.toUpperCase().startsWith("2G")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " where longitude > 1 and signal_strength < 0 and "
						+ "(net_type like '%EDGE%' or net_type like '%GPRS%' or net_type like '%GSM%')  limit  "+start+" ,  "+end+" ";
			} else if (type.toUpperCase().startsWith("3G")) {
				sql = "select longitude lng,latitude lat ,  province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " " + "where longitude > 1 and signal_strength < 0 and net_type like '%TD-S%' "
						+ "  limit  "+start+" ,  "+end+"";
			} else if (type.toUpperCase().startsWith("4G")) {
				sql = "select longitude lng,latitude lat ,  province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and signal_strength < 0 and net_type like '%LTE%' "
						+ "   limit  "+start+" ,  "+end+"";
			} else if (type.toUpperCase().startsWith("WIFI")) {
				sql = "select longitude lng,latitude lat ,  province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and signal_strength < 0 and net_type like '%WIFI%' "
						+ "   limit  "+start+" ,  "+end+"";
			}
			// 若不是当月的时间，则执行全月的数据
		} else {
			if (type.toUpperCase().startsWith("2G")) {
				sql = "select longitude lng,latitude lat ,  province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " where longitude > 1 and signal_strength < 0 and "
						+ "(net_type like '%EDGE%' or net_type like '%GPRS%' or net_type like '%GSM%')    limit  "+start+" ,  "+end+"";// limit 0 , 100
			} else if (type.toUpperCase().startsWith("3G")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " " + "where longitude > 1 and signal_strength < 0 and net_type like '%TD-S%' "
						+ "  limit  "+start+" ,  "+end+"";
			} else if (type.toUpperCase().startsWith("4G")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and signal_strength < 0 and net_type like '%LTE%' "
						+ "   limit  "+start+" ,  "+end+"";
			} else if (type.toUpperCase().startsWith("WIFI")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and signal_strength < 0 and net_type like '%WIFI%' "
						+ "   limit  "+start+" ,  "+end+"";
			}
		}
		try {
			this.close();
			try {
				String url = srcurl.substring(0, srcurl.lastIndexOf("/") + 1) + basename;
				this.start(url, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(sql + "  :::查询总量sql");
			rs = statement.executeQuery(sql);

			fals = false;
			while (rs.next()) {
				fals = true;
				Map<String, String> map = new HashMap<String, String>();
				String lng = rs.getString("lng");
				String lat = rs.getString("lat");
				String signalstrength = rs.getString(pointer);
				map.put("lng", lng);
				map.put("lat", lat);
				map.put(pointer, signalstrength);
				map.put("province", rs.getString("province"));
				map.put("city", rs.getString("city"));
				map.put("district", rs.getString("district"));
				map.put("street", rs.getString("street"));
				map.put("street_number", rs.getString("street_number"));
				map.put("formatted_address", rs.getString("formatted_address"));
				map.put("city_code", rs.getString("city_code"));
				listMap.add(map);
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
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return listMap;
	}

	/**
	 * 根据type、tdate、数据源表名、数据库名查询数据
	 * 
	 * @param type
	 * @param tdate
	 * @param srctablename
	 * @param basename
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getPingGDate(String type, String tdate, String srctablename, String basename, String pointer ,int start ,int end) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";
		String tomonFirst = "";
		String nextmonFirst = "";
		Calendar calendar = Calendar.getInstance();// 获取系统当前时间
		String todate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
		calendar.add(Calendar.DATE, -1); // 得到前一天
		String yestedayDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		if (!tdate.isEmpty()) {
			// 如果配置中的时间是在本月中，则我们取得数据截止到当天的凌晨
			// 获取的是本月的第一天
			tomonFirst = UtilDate.getFirstDayOfMonth(tdate);
			// 获取下一个月的第一天
			nextmonFirst = UtilDate.getFirstDayOfNextMonth(tdate);
		}
		// 如果是没有配置月份，则该程序执行的是前一天的数据
		if (tdate.isEmpty()) {
			if (type.contains("2G")) {
				sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code,  " + pointer + "  from " + srctablename + " where longitude > 1 and " + pointer + " > 0 and "
						+ "(net_type like '%EDGE%' or net_type like '%GPRS%' or net_type like '%GSM%')   limit  "+start+" ,  "+end+"";
			} else if (type.contains("3G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " " + "where longitude > 1 and " + pointer + " > 0 and net_type like '%TD-S%' "
						+ "  limit  "+start+" ,  "+end+"";
			} else if (type.contains("4G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and " + pointer + " > 0 and net_type like '%LTE%' "
						+ "   limit  "+start+" ,  "+end+"";
			} else if (type.contains("WIFI")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and " + pointer + " > 0 and net_type like '%WIFI%' "
						+ "  limit  "+start+" ,  "+end+"";
			}
			// 如果配置的时间是当月的时间，则数据是从月初的第一天到当前时间的凌晨，也就是到前一天的数据
		} else if (tdate.equals(month)) {
			if (type.contains("2G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " where longitude > 1 and " + pointer + " > 0 and "
						+ "(net_type like '%EDGE%' or net_type like '%GPRS%' or net_type like '%GSM%')  limit  "+start+" ,  "+end+"";
			} else if (type.contains("3G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " " + "where longitude > 1 and " + pointer + " > 0 and net_type like '%TD-S%' "
						+ "  limit  "+start+" ,  "+end+"";
			} else if (type.contains("4G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and " + pointer + " > 0 and net_type like '%LTE%' "
						+ "  limit  "+start+" ,  "+end+" ";
			} else if (type.contains("WIFI")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and " + pointer + " > 0 and net_type like '%WIFI%' "
						+ "  limit  "+start+" ,  "+end+"";
			}
			// 若不是当月的时间，则执行全月的数据
		} else {
			if (type.contains("2G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " where longitude > 1 and " + pointer + " > 0 and "
						+ "(net_type like '%EDGE%' or net_type like '%GPRS%' or net_type like '%GSM%')  limit  "+start+" ,  "+end+"";
			} else if (type.contains("3G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + " " + "where longitude > 1 and " + pointer + " > 0 and net_type like '%TD-S%' "
						+ "  limit  "+start+" ,  "+end+"";
			} else if (type.contains("4G")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and " + pointer + " > 0 and net_type like '%LTE%' "
						+ "  limit  "+start+" ,  "+end+"";
			} else if (type.contains("WIFI")) {
				sql = "select longitude lng,latitude lat ,province,city,district,street,street_number,formatted_address,city_code, " + pointer + "  from " + srctablename + "" + " where longitude > 1 and " + pointer + " > 0 and net_type like '%WIFI%' "
						+ "  limit  "+start+" ,  "+end+"";
			}
		}
		ResultSet rs = null;
		try {
			this.close();
			try {
				String url = srcurl.substring(0, srcurl.lastIndexOf("/") + 1) + basename;
				this.start(url, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(sql + "  :::查询总量sql");
			rs = statement.executeQuery(sql);

			fals = false;
			
			while (rs.next()) {
				fals = true ;
				Map<String, String> map = new HashMap<String, String>();
				String lng = rs.getString("lng");
				String lat = rs.getString("lat");
				String signalstrength = rs.getString(pointer);
				map.put("lng", lng);
				map.put("lat", lat);
				map.put(pointer, signalstrength);
				map.put("province", rs.getString("province"));
				map.put("city", rs.getString("city"));
				map.put("district", rs.getString("district"));
				map.put("street", rs.getString("street"));
				map.put("street_number", rs.getString("street_number"));
				map.put("formatted_address", rs.getString("formatted_address"));
				map.put("city_code", rs.getString("city_code"));
				listMap.add(map);
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
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return listMap;
	}

	/**
	 * 获取需网格化的数据
	 * 
	 * @param type
	 * @param tdate
	 * @param basename
	 * @param srctablename
	 * @param pointer
	 * @return
	 * @return List<Map>
	 */
	public List<Map> getOtherData(String type, String tdate, String basename, String srctablename, String pointer ,int start ,int end) {

		List<Map> listMap = new ArrayList<Map>();
		String sql = "";
		String tomonFirst = "";
		String nextmonFirst = "";
		ResultSet rs = null;
		Calendar calendar = Calendar.getInstance();// 获取系统当前时间
		String todate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
		calendar.add(Calendar.DATE, -1); // 得到前一天
		String yestedayDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
		// 如果配置中的时间是在本月中，则我们取得数据截止到当天的凌晨
		// 获取的是本月的第一天
		if (!tdate.isEmpty()) {
			// 如果配置中的时间是在本月中，则我们取得数据截止到当天的凌晨
			// 获取的是本月的第一天
			tomonFirst = UtilDate.getFirstDayOfMonth(tdate);
			// 获取下一个月的第一天
			nextmonFirst = UtilDate.getFirstDayOfNextMonth(tdate);
		}
		// 如果是没有配置月份，则该程序执行的是前一天的数据
		if (tdate.isEmpty()) {
			sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code,  " + pointer + "  from " + srctablename + " where longitude > 1  and " + pointer + " >0 and province!='' and province!='-' limit  "+start+" ,  "+end+"";
			// "')*1000 and time <UNIX_TIMESTAMP('2015-11-06')*1000  and "+pointer+" is not null ";

			// 如果配置的时间是当月的时间，则数据是从月初的第一天到当前时间的凌晨，也就是到前一天的数据
		} else if (tdate.equals(month)) {// and "+pointer+" not like '%,%' and
											// "+pointer+" is not null
			sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code," + pointer + "  from " + srctablename + " where longitude > 1     and " + pointer + " >0  and province!='' and province!='-' limit  "+start+" ,  "+end+"";
			// 若不是当月的时间，则执行全月的数据
		} else {
			sql = "select longitude lng,latitude lat , province,city,district,street,street_number,formatted_address,city_code," + pointer + "  from " + srctablename + " where longitude > 1   and " + pointer + " >0 and province!='' and province!='-' limit  "+start+" ,  "+end+"";
		}
		try {
			this.close();
			try {
				String url = srcurl.substring(0, srcurl.lastIndexOf("/") + 1) + basename;
				this.start(url, srcuser, srcpassword);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(sql + " ::查询ping、http、speed_test sql");
			rs = statement.executeQuery(sql);

			fals = false;
			while (rs.next()) {
				fals = true;
				Map<String, String> map = new HashMap<String, String>();
				String lng = rs.getString("lng");
				String lat = rs.getString("lat");
				String pointerdata = rs.getString(pointer);
				map.put("lng", lng);
				map.put("lat", lat);
				map.put("province", rs.getString("province"));
				map.put("city", rs.getString("city"));
				map.put("district", rs.getString("district"));
				map.put("street", rs.getString("street"));
				map.put("street_number", rs.getString("street_number"));
				map.put("formatted_address", rs.getString("formatted_address"));
				map.put("city_code", rs.getString("city_code"));
				map.put(pointer, pointerdata);
				listMap.add(map);
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
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return listMap;
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
			this.close();
			start(url, dstuser, dstpassword);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean flag = false;
		ResultSet resultSet = null;
		DatabaseMetaData databaseMetaData;
		try {
			databaseMetaData = (DatabaseMetaData) conn.getMetaData();
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
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	public boolean creatNewTable(String newTableName, String dataBasename, String pointer) {

		boolean flag = true;
		String sql = "";
		if (newTableName.toLowerCase().startsWith("2g") || newTableName.toLowerCase().startsWith("3g") || newTableName.toLowerCase().startsWith("4g")||newTableName.toLowerCase().startsWith("wifi")) {
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT, " + "lng decimal(10,6) DEFAULT NULL, lat decimal(30,6) DEFAULT NULL,"
					+ "test_times varchar(20) DEFAULT NULL," + "avg_signalstrength decimal(10,3) DEFAULT NULL, " +
					"`province`  varchar(200)  DEFAULT NULL,`city`  varchar(200)   DEFAULT NULL ,"
					+ "`district`  varchar(200)  DEFAULT NULL ,`street`  varchar(200)  DEFAULT NULL ," +
					"`street_number` varchar(200) DEFAULT NULL ,`formatted_address` varchar(200) DEFAULT NULL ,`city_code`  varchar(200)  DEFAULT NULL ,"+
					"PRIMARY KEY (id),INDEX latlng(lng,lat)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		} else {
			sql = "CREATE TABLE " + newTableName + " ( id int(10) NOT NULL AUTO_INCREMENT," + "`lng`  decimal(10,6)  DEFAULT NULL ,`lat`  decimal(10,6)  DEFAULT NULL ,"
					+ "`test_times`  int(10)  DEFAULT NULL ,`" + pointer + "`  decimal(30,3)  DEFAULT NULL ," + "`province`  varchar(200)  DEFAULT NULL,`city`  varchar(200)   DEFAULT NULL ,"
					+ "`district`  varchar(200)  DEFAULT NULL ,`street`  varchar(200)  DEFAULT NULL ," +
					"`street_number` varchar(200) DEFAULT NULL ,`formatted_address` varchar(200) DEFAULT NULL ,`city_code`  varchar(200)  DEFAULT NULL ,"
					+ "PRIMARY KEY (id),INDEX latlng(lng,lat)) CHARSET=utf8 ROW_FORMAT=COMPACT;";
		}

		String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dataBasename;
		System.out.println(sql + "::::::建表sql");
		try {
			start(url, dstuser, dstpassword);
			statement.execute(sql);
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	
	public int countTable(String tableName,String databasename){
		String sql = "select count(id) count from "+tableName+"";
		
		ResultSet rs = null;
		int count = 0;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + databasename;
			start(url, dstuser, dstpassword);
			rs = statement.executeQuery(sql);
			while(rs.next()){
				count = rs.getInt("count");
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}
	/**
	 * 复制上一个月表的数据到最新的月份
	 * 
	 * @param table
	 * @param table_domain
	 */
	public void copyPreviousMonData(String newtablename, String previoustable, String databasename) {
		
		
		int count = countTable(previoustable, databasename);
		int start = 0;
		int end = 20000;
		
		boolean flage = true;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + databasename;
			start(url, dstuser, dstpassword);
			while(flage){
				String copySql = "insert into " + newtablename + " select * from " + previoustable  +" limit "+start+" , "+end+"";
				System.out.println(copySql+"  :::::copySql数据复制sql");
				this.insert(copySql);
				start +=20000;
				//end +=100;
				if(start>count){
					flage= false;
				}
			}
			System.out.println("复制表" + previoustable + "到" + newtablename + "成功");
		} catch (Exception e) {
			System.out.println("复制表" + previoustable + "到" + newtablename + "失败");
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 根据配置文件中的map等级进行数据库查询对应的lng、lat的值
	 * 
	 * @param mapLevel
	 * @return
	 * @return List
	 */
	public List getLNGLATByLevel(String mapLevel, String databasename) {
		List list = new ArrayList();
		String sql = "SELECT map.loninterval lng,map.latinterval lat FROM map_level map where level = " + mapLevel;
		double lng = 0;
		double lat = 0;
		ResultSet rs = null;
		System.out.println(sql);
		try {
			this.close();
			start(dsturl, dstuser, dstpassword);

			rs = statement.executeQuery(sql);
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
				this.close();
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
	public Map queryDataExit(String ln, String la,Connection gpsconn) {
		//_location
		String sql = "select * from  gps_location  where lng='" + ln + "' and lat='" + la + "'";
		Map map = new HashMap();
		ResultSet rs = null;
		boolean fa = false;
		try {
			//start(dsturl, dstuser, dstpassword);
			if(gpsconn==null){
				gpsconn = DatabaseUtil.getconn(dsturl, dstuser, dstpassword);
			}
			statement = gpsconn.createStatement();
			rs = statement.executeQuery(sql);
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
				this.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		map.put("fa", fa);
		return map;
	}

	/**
	 * 静态sql的批量插入
	 * 
	 * @param sqlList
	 * @return
	 */
	public boolean insertList(List<String> sqllist, String dataBaseName) {

		// 获取数据库连接
		int count = 0;
		boolean flag = false;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + dataBaseName;
			try {
				close();
				start(url, dstuser, dstpassword);
			} catch (Exception e) {
				e.printStackTrace();
			}
			conn.setAutoCommit(false);
			for (Iterator iterator = sqllist.iterator(); iterator.hasNext();) {
				String sql = (String) iterator.next();
				// count++;
				System.out.println(sql + "  :::::插入sql");
				statement.addBatch((String) sql);
				// 10000条记录插入一次
				count++;
				// System.out.println(count);
				/*if (count % 10000 == 0) {
					statement.executeBatch();
					conn.commit();
				}*/
			}
			// 不足10000的最后进行插入
			statement.executeBatch();
			conn.commit();
			flag = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return flag;
	}

	// -------------------------------------------start-处理地图信息----------------------------------------------------
	
	/**
	 * 查询ter表中是否存在
	 * 
	 * @param table
	 * @param lng
	 * @param lat
	 * @return
	 * @return boolean
	 */
	public Map queryTher(String table, String lng, String lat, String databasename, String pointer,Connection connn) {

		String sql = "select * from " + table + " where lng = '" + lng + "' and lat = " + lat;
		Map map = new HashMap();
		System.out.println(sql + " :::根据经纬度查询数据是否存在");
		boolean flage = false;
		ResultSet rs = null;
		try {
			String url = dsturl.substring(0, dsturl.lastIndexOf("/") + 1) + databasename;
			try {
				//start(url, dstuser, dstpassword);
				if(connn==null){
					connn = DatabaseUtil.getconn(url, dstuser, dstpassword);
				}
				statement = connn.createStatement();
			} catch (Exception e) {
				e.printStackTrace();
			}
			rs = (ResultSet) statement.executeQuery(sql);
			if (rs.next()) {
				flage = true;
				map.put("id", rs.getString("id"));
				map.put("test_times", rs.getInt("test_times"));
				if (table.toLowerCase().startsWith("2g") || table.toLowerCase().startsWith("3g") || table.toLowerCase().startsWith("4g")|| table.toLowerCase().startsWith("wifi")) {
					map.put(pointer, rs.getDouble("avg_signalstrength"));
				} else {
					map.put(pointer, rs.getDouble(pointer));
				}

			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
			if (rs != null) {
					rs.close();
			}
			if(statement!=null){
				statement.close();
			}
			if(connn!=null){
				connn.close();
			}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		map.put("flag", flage);
		return map;
	}

	
	public Map<String, Map> getGPSLocation(){
		Map<String, Map> gpsMap = new HashMap<String, Map>();
		String sql = "select *  from gps_location_2";
		ResultSet rs = null;
		try {
			this.start(dsturl, dstuser, dstpassword);
			rs = statement.executeQuery(sql);
			
			while(rs.next()){
				Map map = new HashMap();
				String lng = rs.getDouble("lng")+"" ;
				String lat = rs.getDouble("lat")+"";
				String province = rs.getString("province");
				String city = rs.getString("city");
				String district = rs.getString("district");
				String street = rs.getString("street");
				String city_code = rs.getString("city_code");
				String formatted_address = rs.getString("formatted_address");
				map.put("province",province);
				map.put("city", city);
				map.put("district", district);
				map.put("street", street);
				map.put("cityCode",city_code);
				map.put("formatted_address",formatted_address);
				
				gpsMap.put(lng+","+lat, map);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}finally{
			try {
				this.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return gpsMap;
	}
	
	// -------------------------------------------------------end-处理地图信息--------------------------------------------------
	/**
	 * 判断字符串是否是整数
	 */
	public static boolean isInteger(String value) {
		try {
			Integer.parseInt(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * 判断字符串是否是浮点数
	 */
	public static boolean isDouble(String value) {
		try {
			Double.parseDouble(value);
			if (value.contains("."))
				return true;
			return false;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * 判断字符串是否是数字
	 */
	public static boolean isNumber(String value) {
		return isInteger(value) || isDouble(value);
	}

}
