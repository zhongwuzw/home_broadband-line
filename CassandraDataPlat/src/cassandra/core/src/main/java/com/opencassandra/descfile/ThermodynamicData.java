package com.opencassandra.descfile;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import com.opencassandra.v01.dao.impl.BdFix;
import com.opencassandra.v01.dao.impl.Earth2Mars;
import com.opencassandra.v01.dao.impl.ThermodynamicDao;

/**
 * 处理信号强度热力图数据
 * 
 * @author：kxc
 * @date：Oct 29, 2015
 */
public class ThermodynamicData {

	// 获取配置的nettype类型
	private static String[] nettypes = ConfParser.nettype;
	// 获取配置中的map等级
	private static String[] maplevel = ConfParser.mapLevel;
	// 获取配置的时间
	private static String[] testdate = ConfParser.testdate;

	private static Map lnlaMap = new HashMap();
	private static String[] codes = ConfParser.code;
	private static double ln = 0;
	private static double la = 0;
	// 格式化信号强度及经纬度
	static Format fm = new DecimalFormat("#.######");

	// ----------------------------------------------------------------------------------------------------------------

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		ThermodynamicDao thermodynamicDao = new ThermodynamicDao();
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		ThermodynamicData tdata = new ThermodynamicData();

		long startdate = 0;
		long enddate = 0;
		// 获取配置文件中的nettype类型
		if (nettypes != null && nettypes.length > 0) {
			// 遍历 配置文件中date
			startdate = new Date().getTime();
			System.out.println("开始进行数据读取::::::  " + startdate);
			String dbName = "";
			// 遍历配置code
			for (int m = 0; m < codes.length; m++) {
				String code = codes[m];
				try {
					// 关闭数据库
					thermodynamicDao.close();

					if (!ConfParser.dataurl.endsWith("/")) {
						System.out.println(ConfParser.dataurl);
						if (m > 0) {
							if (ConfParser.dataurl.contains("_" + ConfParser.code[m - 1])) {
								ConfParser.dataurl = ConfParser.dataurl.replace("_" + ConfParser.code[m - 1], "");
							}
						}
						dbName = ConfParser.dataurl.substring(ConfParser.dataurl.lastIndexOf("/") + 1, ConfParser.dataurl.length());
						dbName = dbName + "_" + code;
						ConfParser.dataurl = ConfParser.dataurl + "_" + code;
					}

					try {
						// 建立数据库连接
						thermodynamicDao.datastart();
					} catch (MySQLSyntaxErrorException e) {
						// 创建数据库
						thermodynamicDao.createDB(dbName);
						try {
							thermodynamicDao.datastart();
						} catch (Exception e1) {
							e1.printStackTrace();
						}
					}
					try {
						// /关闭数据库
						thermodynamicDao.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				} catch (Exception e) {
					// TODO: handle exception
					e.printStackTrace();
				}
				// 遍历遍历配置时间
				for (int k = 0; k < testdate.length; k++) {
					String dates = testdate[k];
					// 遍历nettype（2G,3G,4G）
					for (int i = 0; i < nettypes.length; i++) {
						String netty = nettypes[i];
						try {
							thermodynamicDao.close();
							System.out.println("：：：：：数据库关闭");
						} catch (SQLException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						list = thermodynamicDao.getTypeData("ping_new", netty, code);
						// 遍历map等级
						for (int j = 0; j < maplevel.length; j++) {
							// 2Gsignalstrength_201510_1
							String therTable = netty + "signalstrength_" + dates + "_" + maplevel[j];

							try {
								// 查询表是否存在
								boolean fal = thermodynamicDao.queryTableExist(therTable);
								if (!fal) {
									// 不存在则创建该表
									thermodynamicDao.createTherTab(therTable);
								}
							} catch (Exception e) {
								e.printStackTrace();
							}

							// 根据nettype查询ping_new 中的条件数据
							tdata.getListMap(list, maplevel[j], therTable);
							// 在一个map等级结束后，把不是1000倍数的数据执行完毕
							tdata.maptolist(therTable);
						}
					}
				}
			}
		}
		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：： " + enddate + " ::  " + new Date() + "  ::用时：：：  " + (enddate - startdate));

	}

	/**
	 * 处理从ping_new表中取出来的数据，把数据以lnglat为key，次数及平均强度为值放入map中
	 * 并且，如果有相同的key值时，则进行次数的增加和平均强度的计算，然后再用该key存放新的value值
	 * 
	 * @param list
	 * @param maplevel
	 * @return void
	 */
	public void getListMap(List<Map<String, String>> list, String maplevel, String therTable) {

		ThermodynamicDao thermodynamicDao = new ThermodynamicDao();
		List<String> sqlList = new ArrayList<String>();
		String sql = "";
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			Map<String, String> map = (Map<String, String>) iterator.next();
			String lng = map.get("lng");
			String lat = map.get("lat");
			String signalstrength = map.get("signalstrength");

			if (!lng.isEmpty() && !lat.isEmpty() && lat.indexOf(",") < 0) {
				ln = Double.parseDouble(lng);
				la = Double.parseDouble(lat);
			}

			// 处理经纬度数据
			double[] em = Earth2Mars.transform(ln, la);
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			double longitudeNum = bf[0];
			double latitudeNum = bf[1];
			List lnglatlist = thermodynamicDao.getLNGLATByLevel(maplevel);
			double LNGinterval = (Double) lnglatlist.get(0);
			double LATinterval = (Double) lnglatlist.get(1);
			// 计算网格化数据
			ln = ((int) (longitudeNum / LNGinterval)) * LNGinterval + LNGinterval / 2;
			la = ((int) (latitudeNum / LATinterval)) * LATinterval + LATinterval / 2;
			// 查询表中该网格化后的经纬度数据是否已经存在e
			// Map nummap = thermodynamicDao.queryTher(therTable, fm.format(ln),
			// fm.format(la));
			// boolean flage = (Boolean) nummap.get("flag");
			String key = fm.format(ln) + "," + fm.format(la);
			if (lnlaMap.containsKey(key)) {
				String val = (String) lnlaMap.get(key);
				String valsig = val.substring(0, val.indexOf(","));
				String times = val.substring(val.indexOf(",") + 1, val.length());
				int testtimes = Integer.parseInt(times);
				double sign = Double.parseDouble(valsig);
				double avgsing = (sign * testtimes + Double.parseDouble(signalstrength)) / (testtimes + 1);
				testtimes = testtimes + 1;

				lnlaMap.put(fm.format(ln) + "," + fm.format(la), avgsing + "," + testtimes);

			} else {
				lnlaMap.put(fm.format(ln) + "," + fm.format(la), signalstrength + "," + "1");
			}
			if (list.size() > 0) {

				// tdata.getListMap(list,maplevel[j]);
				if (lnlaMap.size() % 1000 == 0) {
					maptolist(therTable);
				}
			}
		}
	}

	/**
	 * 对数据进行查重并进行数据插入
	 * 
	 * @param therTable
	 * @return void
	 */
	public void maptolist(String therTable) {
		ThermodynamicDao thermodynamicDao = new ThermodynamicDao();
		String sql = "";
		List<String> sqlList = new ArrayList<String>();
		Iterator iter = lnlaMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			String lnglat = entry.getKey().toString();
			String val = entry.getValue().toString();
			ln = Double.parseDouble(lnglat.substring(0, lnglat.indexOf(",")));
			la = Double.parseDouble(lnglat.substring(lnglat.indexOf(",") + 1, lnglat.length()));
			double avg = Double.parseDouble(val.substring(0, val.indexOf(",")));
			int times = Integer.parseInt(val.substring(val.indexOf(",") + 1, val.length()));
			// 进行数据查询，看该经纬度下的数据是否已经存在
			Map nummap = thermodynamicDao.queryTher(therTable, fm.format(ln), fm.format(la));
			boolean flage = (Boolean) nummap.get("flag");
			if (!flage) {
				sql = "insert into " + therTable + " (lng,lat,test_times,avg_signalstrength) values('" + fm.format(ln) + "','" + fm.format(la) + "','" + times + "','" + fm.format(avg) + "')";
			} else {
				// 若存在，则把测试次数test_times加上已经算过的次数 ，并计算平均的信号强度
				// 执行更新操作
				double avg_signalstrength = (Double) nummap.get("avg_signalstrength");
				int terminalnum = (Integer) nummap.get("test_times");
				// 计算平均信号强度
				avg_signalstrength = (avg_signalstrength * terminalnum + avg * times) / (terminalnum + times);
				terminalnum = terminalnum + times;
				sql = "update " + therTable + " set test_times='" + terminalnum + "',avg_signalstrength='" + fm.format(avg_signalstrength) + "' where id=" + nummap.get("id");
			}
			sqlList.add(sql);
		}
		// 插入数据
		thermodynamicDao.insertList(sqlList);
		// 清空list
		sqlList.clear();
		// 清空map
		lnlaMap.clear();
	}

}