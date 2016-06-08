package com.opencassandra.areachart;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.utils.UtilDate;

/**
 * 处理一定级别的网格化数据区域图
 * 
 * @author：kxc
 * @date：Nov 26, 2015
 */
public class AreaChartData {

	// 配置中的code
	static String[] codes = ConfParser.code;
	// 配置的时间（月份）
	static String[] testdates = ConfParser.testdate;
	// 配置的地图级别
	static String[] mapLevels = ConfParser.mapLevel;
	// 配置的表标识
	static String[] pointertypes = ConfParser.pointertype;
	// 配置的省市区标识
	static String[] pointers = ConfParser.pointer;

	static AreaChartDao areaChartDao = new AreaChartDao();
	// 该rex用于分解pointertypes
	private static Pattern pattern = Pattern.compile("[|]");

	private static String basename = ConfParser.basename;
	/**
	 * 处理区域图数据
	 * 
	 * @param args
	 * @return void
	 */
	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args) {

		// 程序开始时间
		// long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		// startdate = new Date();
		Date start = new Date();
		System.out.println(UtilDate.dateToStrLong(new Date()) + "  ::开始进行数据读取::::::  " + start.getTime());
		// 循环code
		for (int i = 0; i < codes.length; i++) {
			String code = codes[i];// 确定数据源库

			String srcBaseName = basename + code;
			areaChartDao.startOrCreateDB(code, srcBaseName);
			// 根据code查询对应的数据名,该数据库是数据源数据库
			// String dataBasename = areaChartDao.getDatabaseName(code);
			// 循环配置时间（月份）
			for (int j = 0; j < testdates.length; j++) {
				String tdate = testdates[j];

				if (tdate.isEmpty()) {
					Calendar calendar = Calendar.getInstance();// 获取系统当前时间
					String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
					tdate = month;
				}
				// 获取上月份时间
				String preMon = UtilDate.getPreviousMonth(tdate);

				// 循环省市区标识字段
				for (int k = 0; k < pointertypes.length; k++) {
					String pointertype = pointertypes[k];
					String types[] = pattern.split(pointertype);
					// 从遍历出来的值中把表标识及字段对应值分离出来
					String type = types[0];// 表标识
					String tabPointer = types[1];// 对应字段名

					// 拼接数据源表名
					String srcTableName = type + "_" + tdate + "_1";

					// 循环省市区标识字段
					for (int m = 0; m < pointers.length; m++) {
						String pointer = pointers[m];
						// 根据标识字段查询区域图数据
						List dataList = areaChartDao.getDataList(srcTableName, srcBaseName, pointer, tabPointer);
						// 拼接区域图数据入库表名
						String newTableName = pointer + "_" + type;

						if (dataList.size() > 0) {
							// 查询该区域表是否已存在
							boolean flage = areaChartDao.queryTableExit(newTableName, srcBaseName);

							if (!flage) {
								// 不存在则创建
								areaChartDao.creatNewTable(newTableName, srcBaseName, tabPointer);
							} else {
								// 若已存在则应删除该月份的数据
								areaChartDao.delTodMonData(newTableName, srcBaseName, tdate);
							}
							boolean fa = areaChartDao.queryPreMon(newTableName, srcBaseName, preMon);
							if (fa) {
								int maxId = areaChartDao.queryMaxId(newTableName, srcBaseName);
								areaChartDao.copyPreToMon(newTableName, srcBaseName, tabPointer, tdate, preMon, maxId);
							}
							// 处理查询出的区域图数据
							areaChartDao.dealData(dataList, newTableName, srcBaseName, tabPointer, tdate);
						}
					}
				}
			}
		}

		enddate = new Date().getTime();
		System.out.println(UtilDate.dateToStrLong(start) + " :开始时间: " + UtilDate.dateToStrLong(new Date()) + " :结束时间: " + " :用时: " + (enddate - start.getTime()));
	}
}
