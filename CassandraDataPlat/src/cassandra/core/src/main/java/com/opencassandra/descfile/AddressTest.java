package com.opencassandra.descfile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

public class AddressTest {
	Statement statement;
	Connection conn;
	private static String ak = ConfParser.ak;
	private static String sql = "";
	public static void main(String[] args) {
		AddressTest addrestTest = new AddressTest();
		addrestTest.test();
	}
	public void test() {
		Format fm = new DecimalFormat("#.###");
		String spaceGpsStr = "";
		String latitude = "";
		String longitude = "";
		String resp = "";
		
		String provinceString = "";
		String districtString = "";
		String cityString = "";
		String street = "";
		String streetNum = "";
		try {
			Map map = new HashMap();
			for (double i = 17.712; i < 54; i=i+0.001) {
				for (double j = 73.000; j < 136; j=j+0.001) {
					double latitudeNum = i;
					double longitudeNum = j;
					latitude = fm.format(latitudeNum);
					longitude = fm.format(longitudeNum);
					
					System.out.println(latitude+","+longitude);
					try {
						resp = testPost(latitude, longitude);
						spaceGpsStr = subLalo(resp);
						String spaces[] = spaceGpsStr.split("_");
						provinceString = spaces[0];
						cityString = spaces[1];
						districtString = spaces[2];
						street = spaces[3];
						streetNum = spaces[4];
						if(provinceString.isEmpty() || provinceString.equals("-")){
							continue;
						}
						map.put("latitude", latitude);
						map.put("longitude", longitude);
						if(!provinceString.isEmpty()){
							map.put("province", provinceString);
						}
						if(!districtString.isEmpty()){
							map.put("district", districtString);
						}
						if(!cityString.isEmpty()){
							map.put("city", cityString);
						}
						if(!street.isEmpty()){
							map.put("street", street);
						}
						if(!streetNum.isEmpty()){
							map.put("streetNum", streetNum);
						}
						sql = appendSql(map).toString();
						if(sql.isEmpty()){
							continue;
						}
						try {
							start();	
							insert(sql);
						} catch (Exception e) {
							e.printStackTrace();
						}finally{
							close();
						}
					} catch (Exception e) {
						continue;
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void start() {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
//		String url = "jdbc:mysql://218.206.179.109:3306/testdataanalyse";
		String user = ConfParser.user;
		String password = ConfParser.password;
//		String password = "Bi123456";
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			statement = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() throws SQLException {
		if (conn != null) {
			conn.close();
		}
	}
	
	public boolean insert(String sql) {
		System.out.println("sql:" + sql);
		boolean flag = false;
		if(sql.isEmpty()){
			flag = false;
		}else{
			try {
				flag = statement.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;
			}	
		}
		return flag;
	}
	
	private static StringBuffer appendSql(Map map) {
		StringBuffer sql = new StringBuffer("");
		String database = "testdataanalyse";
		StringBuffer columnStr = new StringBuffer("(");
		StringBuffer valueStr = new StringBuffer(" values (");
		StringBuffer updateStr = new StringBuffer("");
		String table = "address_info";
		sql.append("insert into " + database + "." + table + " ");
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
				columnStr.append(name + ",");
				valueStr.append("'" + value + "',");
			}
			while (columnStr.toString().trim().endsWith(",")) {
				columnStr = new StringBuffer(columnStr.toString().substring(0,
						columnStr.toString().lastIndexOf(",")));
			}
			while (valueStr.toString().trim().endsWith(",")) {
				valueStr = new StringBuffer(valueStr.toString().substring(0,
						valueStr.toString().lastIndexOf(",")));
			}
			columnStr.append(" )");
			valueStr.append(" )");
			sql.append(columnStr);
			sql.append(valueStr);
		} catch (Exception e) {
			System.out.println("拼装语句错误：" + sql);
			sql = new StringBuffer("");
			e.printStackTrace();
		} finally {
		}
		return sql;
	}
	
	public static String testPost(String x, String y) throws IOException {

		URL url = new URL("http://api.map.baidu.com/geocoder?ak="+ ak
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
			if(ssq.endsWith("_")){
				ssq = ssq.substring(0, ssq.length()-1);
			}
			return ssq;
		}
		return null;
	}
}
