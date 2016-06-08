package com.opencassandra.Mesher;

import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.utils.MapAddressUtil;
import com.opencassandra.service.utils.UtilDate;
import com.opencassandra.v01.dao.impl.BdFix;
import com.opencassandra.v01.dao.impl.Earth2Mars;

/**
 * 数据网格化 通用类
 * 
 * @author：kxc
 * @date：Nov 9, 2015
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MesherData {
	// 获取配置中的map等级
	private static String[] maplevel = ConfParser.mapLevel;
	// 获取配置的时间
	private static String[] testdate = ConfParser.testdate;
	// 全局的用于装配以及解析数据使用
	private static Map lnlaMap = new HashMap();
	// 获取配置的code
	private static String[] codes = ConfParser.code;
	
	//获取数据入库库名前缀
	private static String base_prefix = ConfParser.base_prefix;
	
	// 获取配置中的表标识和字段
	private static String[] pointertypes = ConfParser.pointertype;
	// 该rex用于分解pointertypes
	private static Pattern pattern = Pattern.compile("[|]");
	private static double ln = 0;
	private static double la = 0;
	// 格式化信号强度及经纬度
	static Format fm = new DecimalFormat("#.######");
	// 百度开关
	private static String baiduSwitch = ConfParser.baiduSwitch;
	private static MesherDao mesherDao = new MesherDao();
	private static Map<String, Map> gpsMap = new HashMap<String, Map>();

	private static List lnglatlist = new ArrayList();
	//---------------------------------------------------------------------------------------------------------------------
	public static void main(String[] args) {
		List<Map> listMap = new ArrayList<Map>();
		MesherData mesherData = new MesherData();
		// 程序开始时间
		long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		boolean dayFlage = true;
		startdate = new Date().getTime();
		System.out.println("开始进行数据读取::::::  " + startdate);
		gpsMap = mesherDao.getGPSLocation();
		// 遍历配置文件中的code
		for (int i = 0; i < codes.length; i++) {
			String code = codes[i];
			// 该数据库名是数据要存入的库
			String srcBaseName = base_prefix+"_"+code;
			mesherDao.startOrCreateDB( srcBaseName);
			// 根据code查询对应的数据名,该数据库是数据源数据库
			String dataBasename = mesherDao.getDatabaseName(code);
			// 首先对配置时间数组进行排序
			Arrays.sort(testdate);
			// 遍历配置时间
			for (int j = 0; j < testdate.length; j++) {
				String tdate = testdate[j];
				// 遍历配置中表标识及对应字段值
				for (int k = 0; k < pointertypes.length; k++) {
					String pointertype = pointertypes[k];
					String types[] = pattern.split(pointertype);
					// 从遍历出来的值中把表标识及字段对应值分离出来
					String type = types[0];// 表标识
					String pointer = types[1];// 对应字段名
					// 现在获取到了code、时间、表标识，所以可以通过查询表对应表查询表名,这是数据源表
					String srctablename = mesherDao.getsrctablename(code, tdate, type,dataBasename);
					// 应先查询该表是否存在
					// 获取查询数据
					int start = 0;
					int end = 100000;
					boolean flag = true;
					while (flag) {
						listMap = mesherDao.getDataSrc(type, tdate, srctablename, dataBasename, pointer, start, end);
						flag = MesherDao.fals;

						if (tdate.isEmpty()) {
							Calendar calendar = Calendar.getInstance();// 获取系统当前时间
							String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
							tdate = month;
							dayFlage = false;
						}

						for (int l = 0; l < maplevel.length; l++) {
							String level = maplevel[l];
							// 数据要入的表名
							String newTableName = type + "_" + tdate + "_" + level;
							lnglatlist = mesherDao.getLNGLATByLevel(level, srcBaseName);
							try {
								// 查询该表是否已经存在
								boolean newFlage = mesherDao.queryTableExit(newTableName, srcBaseName);
								// 获取上月份世间
								String previousMon = UtilDate.getPreviousMonth(tdate);
								String previousTable = type + "_" + previousMon + "_" + level;

								if (!newFlage) {
									// 如果没有要入库的表，先创建该表
									mesherDao.creatNewTable(newTableName, srcBaseName, pointer);

									// 查询上一个月份的表是否存在
									boolean fa = mesherDao.queryTableExit(previousTable, srcBaseName);
									// 如果上月份表存在，则直接复制该月表中的数据到这月份的表中
									if (fa) {
										mesherDao.copyPreviousMonData(newTableName, previousTable, srcBaseName);
									}
								} else {
									// 如果tdate是空的，则该时间执行的是前一天的数据，所以是不用删除该表的
									if (dayFlage && start == 0) {
										// 如果已经存在则删除该表
										mesherDao.delTodMonTable(newTableName, srcBaseName);
										// 再新创建该表
										mesherDao.creatNewTable(newTableName, srcBaseName, pointer);
										// 查询上一个月份的表是否存在
										boolean fa = mesherDao.queryTableExit(previousTable, srcBaseName);
										if (fa) {
											mesherDao.copyPreviousMonData(newTableName, previousTable, srcBaseName);
										}
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}

							// 数据入库表创建完毕后，处理数据
							mesherData.getListMap(listMap, level, newTableName, pointer, srcBaseName);
							// 在一个map等级结束后，把不是1000倍数的数据执行完毕
							mesherData.maptolist(newTableName, pointer, srcBaseName);
							lnglatlist.clear();
						}
						listMap.clear();
						start += 100000;
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
	 * @param newTableName
	 * @param pointer
	 * @param dataBaseName
	 * @return void
	 */
	public void getListMap(List<Map> list, String maplevel, String newTableName, String pointer, String dataBaseName) {
		// 省市地址信息
		String provinceString = null;
		String cityString = null;
		String districtString = null;
		String street = null;
		String cityCode = null;
		String formatted_address = null;
		String resp = "";
		double avgsing = 0;

		boolean queryFlage = false;
		// 遍历listMap数据
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			Map map = (Map) iterator.next();
			// Map dat = new HashMap();
			// 该数据为源数据
			String lng = (String) map.get("lng");
			String lat = (String) map.get("lat");
			// 数据源的平均值
			String srcavg = (String) map.get(pointer);
			String[] srcavgData = null;
			if (srcavg.isEmpty()) {
				continue;
			} else {
				if (srcavg.contains(",")) {
					srcavgData = srcavg.split(",");
				} else {
					srcavgData = new String[] { srcavg };
				}
			}
			// 判断该平均值是否是数字
			if (MesherDao.isNumber(srcavgData[0])) {
				srcavg = srcavgData[0];
			} else {
				continue;
			}
			// 数据源的取出的值
			double avgres = Double.parseDouble(srcavg);
			Map entryMap = new HashMap();
			// 处理异常经纬度信息
			if (lng != null && lat != null && !lng.isEmpty() && !lat.isEmpty() && lat.indexOf(",") < 0) {
				ln = Double.parseDouble(lng);
				la = Double.parseDouble(lat);
			} else {
				continue;
			}

			// 查询完数据地址位置信息开始按照表标识处理数据
			// 处理经纬度数据
			/*double[] em = Earth2Mars.transform(ln, la);
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			double longitudeNum = bf[0];
			double latitudeNum = bf[1];*/

			
			double LNGinterval = (Double) lnglatlist.get(0);
			double LATinterval = (Double) lnglatlist.get(1);

			// 计算网格化数据
			ln = ((int) (ln / LNGinterval)) * LNGinterval + LNGinterval / 2;
			la = ((int) (la / LATinterval)) * LATinterval + LATinterval / 2;

			String key = fm.format(ln) + "," + fm.format(la);
			if (lnlaMap.containsKey(key)) {
				Map value = (Map) lnlaMap.get(key);
				String val = (String) value.get(key);
				String valsig = val.substring(0, val.indexOf(","));
				String times = val.substring(val.indexOf(",") + 1, val.length());
				int testtimes = Integer.parseInt(times);
				// 已存在全局map中的值
				double sign = Double.parseDouble(valsig);
				// 处理ping的平均时延与其他的分开
				// 平均时延的处理公式//(times1+ times2)/(times1/rtt1 + times2/rtt2)
				if (newTableName.contains("ping")) {
					avgsing = (testtimes + 1) / (testtimes / sign + 1 / avgres);
				} else {
					avgsing = (sign * testtimes + avgres) / (testtimes + 1);
				}
				testtimes = testtimes + 1;

				entryMap.put(fm.format(ln) + "," + fm.format(la), avgsing + "," + testtimes);
				provinceString = (String) value.get("province");
				cityString = (String) value.get("city");
				districtString = (String) value.get("district");
				street = (String) value.get("street");
				cityCode = (String) value.get("cityCode");
				formatted_address = (String) value.get("formatted_address");
			} else {
				double lnng = 0;
				double laat = 0;
				if (lng != null && lat != null && !lng.isEmpty() && !lat.isEmpty() && lat.indexOf(",") < 0) {
					lnng = Double.parseDouble(lng);
					laat = Double.parseDouble(lat);
				} else {
					continue;
				}

				// 打开关闭百度接口
				if (!baiduSwitch.isEmpty() && "onplat".equals(baiduSwitch)) {

					double gln = (int) (lnng / 0.001) * 0.001 + 0.001 / 2;
					double gla = (int) (laat / 0.0008) * 0.0008 + 0.0008 / 2;
					Map lalnMap = new HashMap();
					boolean fa = false;
					if (gpsMap.containsKey(fm.format(gln) + "," + fm.format(gla))) {
						lalnMap = gpsMap.get(fm.format(gln) + "," + fm.format(gla));
						fa = true;
					} else {
						lalnMap = mesherDao.queryDataExit(fm.format(gln), fm.format(gla));
						fa = (Boolean) lalnMap.get("fa");
						queryFlage = true;
					}
					// lalnMap =
					// mesherDao.queryDataExit(fm.format(gln),fm.format(gla));
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
								provinceString = spaces[0];
								cityString = spaces[1];
								districtString = spaces[2];
								street = spaces[3];
								// streetNum = spaces[4];
								cityCode = spaces[5];
								formatted_address = spaces[6];
								String sql = "insert into gps_location " +
										"(lng,lat,province,city,district,street,city_code, formatted_address) " +
										"values('" + gln + "','" + gla + "'," + "'" + provinceString + "' ," +
												"'" + cityString+ "','" + districtString + "' ,'" + street + "' ," +
												"'" + cityCode + "' , '"+formatted_address+"')";
								// 把最新的地理位置信息更新到常量表GPS_location中
								mesherDao.updateGPSlocation(sql);

								System.out.println(sql+"  :::::::::::::");
								Map baidumap = new HashMap();
								baidumap.put("province", provinceString);
								baidumap.put("city", cityString);
								baidumap.put("district", districtString);
								baidumap.put("street", street);
								baidumap.put("cityCode", cityCode);
								baidumap.put("formatted_address", formatted_address);
								gpsMap.put(fm.format(gln) + "," + fm.format(gla), baidumap);
							}
						}
					} else {
						System.out.println("  :::该经纬度的数据在GPS_location中已存在：：：");
						provinceString = (String) lalnMap.get("province");
						cityString = (String) lalnMap.get("city");
						districtString = (String) lalnMap.get("district");
						street = (String) lalnMap.get("street");
						cityCode = (String) lalnMap.get("cityCode");
						formatted_address = (String) lalnMap.get("formatted_address");
						if (queryFlage) {
							gpsMap.put(fm.format(gln) + "," + fm.format(gla), lalnMap);
						}
					}
				}

				entryMap.put(fm.format(ln) + "," + fm.format(la), avgres + ",1");
			}
			
			/**
			 * 2015年12月3日 14:16:28
			 * 修改 关于省市区信息为'-'的问题
			 */
			if(provinceString.length()<=1){
				continue;
			}
			entryMap.put("province", provinceString);
			entryMap.put("city", cityString);
			entryMap.put("district", districtString);
			entryMap.put("street", street);
			entryMap.put("cityCode", cityCode);
			entryMap.put("formatted_address", formatted_address);

			lnlaMap.put(fm.format(ln) + "," + fm.format(la), entryMap);
			// System.out.println(lnlaMap.size() + "  ::::::");
			if (list.size() > 0) {
				if (lnlaMap.size() % 100000 == 0) {
					maptolist(newTableName, pointer, dataBaseName);
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
	public void maptolist(String newTableName, String pointer, String databasename) {
		String sql = "";
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
			double avg_map = Double.parseDouble(avgTime.substring(0, avgTime.indexOf(",")));
			int times_map = Integer.parseInt(avgTime.substring(avgTime.indexOf(",") + 1, avgTime.length()));

			// 根据经纬度查询该数据是否已存在
			Map nummap = mesherDao.queryTher(newTableName, fm.format(ln), fm.format(la), databasename, pointer);
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
					if (newTableName.toLowerCase().startsWith("2g") || newTableName.toLowerCase().startsWith("3g") || newTableName.toLowerCase().startsWith("4g")||newTableName.toLowerCase().startsWith("wifi")) {
						sql = "insert into " + newTableName + " (lng,lat,test_times,avg_signalstrength,province,city,district,street,city_code,formatted_address,street_number) values(" + "'" + fm.format(ln) + "','"
								+ fm.format(la) + "','" + times_map + "','" + fm.format(avg_map) + "','" + province + "','" + city + "','" + district + "','" + street + "','" + cityCode + "', '"+formatted_address+"', null )";
					} else {
						sql = "insert into " + newTableName + " (lng,lat,test_times," + pointer + ",province,city,district,street,city_code,formatted_address,street_number) " + "values('" + fm.format(ln) + "','"
								+ fm.format(la) + "','" + times_map + "','" + fm.format(avg_map) + "','" + province + "','" + city + "','" + district + "','" + street + "','" + cityCode + "' , '"+formatted_address+"',null)";
					}
				} else {
					if (newTableName.toLowerCase().startsWith("2g") || newTableName.toLowerCase().startsWith("3g") || newTableName.toLowerCase().startsWith("4g")||newTableName.toLowerCase().startsWith("wifi")) {
						sql = "insert into " + newTableName + " (lng,lat,test_times,avg_signalstrength,province,city,district,street,city_code,formatted_address,street_number) values(" + "'" + fm.format(ln) + "','"
								+ fm.format(la) + "','" + times_map + "','" + fm.format(avg_map) + "'," + province + "," + city + "," + district + "," + street + "," + cityCode + " , "+formatted_address+", null )";
					} else {

						sql = "insert into " + newTableName + " (lng,lat,test_times," + pointer + ",province,city,district,street,city_code,formatted_address,street_number) " + "values('" + fm.format(ln) + "','"
								+ fm.format(la) + "','" + times_map + "','" + fm.format(avg_map) + "'," + province + "," + city + "," + district + "," + street + "," + cityCode + " , "+formatted_address+", null)";
					}
				}

			} else {
				double avgsing = 0;
				// 该数据为已存入数据库的数据
				double avg_base = (Double) nummap.get(pointer);
				int times_base = (Integer) nummap.get("test_times");
				if (newTableName.startsWith("ping")) {
					// 该数据是已经存在数据库中的
					avgsing = (times_base + times_map) / (times_base / avg_base + times_map / avg_map);
				} else {
					avgsing = (avg_base * times_base + avg_map * times_map) / (times_base + times_map);
				}
				times_base = times_base + times_map;
				if (newTableName.toLowerCase().startsWith("2g") || newTableName.toLowerCase().startsWith("3g") || newTableName.toLowerCase().startsWith("4g")||newTableName.toLowerCase().startsWith("wifi")) {
					sql = "update " + newTableName + " set test_times='" + times_base + "', avg_signalstrength ='" + fm.format(avgsing) + " '  where id=" + nummap.get("id");
				} else {
					sql = "update " + newTableName + " set test_times='" + times_base + "'," + pointer + "='" + fm.format(avgsing) + " '  where id=" + nummap.get("id");
				}
			}
			// System.out.println(sql);
			sqlList.add(sql);
		}
		// 插入数据
		mesherDao.insertList(sqlList, databasename);
		// 清空list
		sqlList.clear();
		// 清空map
		lnlaMap.clear();
	}
}
