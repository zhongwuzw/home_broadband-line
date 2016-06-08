package com.opencassandra.terminal;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.descfile.ConfParser;
import com.opencassandra.service.DataService;
import com.opencassandra.service.impl.DataServiceImpl;
import com.opencassandra.service.utils.UtilDate;

/**
 * 网格化终端数据
 * 
 * @author：kxc
 * @date：Nov 19, 2015
 */
@SuppressWarnings({ "rawtypes" })
public class TerminalData {
	// 获取配置中的map等级
	// 获取配置的时间
	private static String[] testdate = ConfParser.testdate;
	// 获取配置的code
	private static String[] codes = ConfParser.code;
	// 获取配置中的表标识和字段
	private static String[] pointertypes = ConfParser.pointertype;
	// 该rex用于分解pointertypes
	private static Pattern pattern = Pattern.compile("[|]");
	// 格式化信号强度及经纬度
	private static String temptable = ConfParser.temptable;

	private static TerminalDao terminalDao = new TerminalDao();

	private static DataService dataServiceImpl = new DataServiceImpl();
	/**
	 * 终端数据网格化处理主方法
	 * 
	 * @param args
	 * @return void
	 */
	public static void main(String[] args) {
		List<Map> listMap = new ArrayList<Map>();
		// 程序开始时间
		long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		startdate = new Date().getTime();
		System.out.println("开始进行数据读取::::::  " + startdate);
		// 遍历配置文件中的code
		for (int i = 0; i < codes.length; i++) {
			String code = codes[i];
			// 该数据库名是数据要存入的库
			String dsBaseName = "terminaltest_" + code;
			dataServiceImpl.startOrCreateDB(code, dsBaseName);
			// 根据code查询对应的数据名,该数据库是数据源数据库
			String dataBasename = terminalDao.getDatabaseName(code);
			// 首先对配置时间数组进行排序
			Arrays.sort(testdate);

			for (int j = 0; j < testdate.length; j++) {
				String tdate = testdate[j];

				String previousMon = UtilDate.getPreviousMonth(tdate);
				// 查询中间表是否存在
				boolean newFlage = terminalDao.queryTableExit(temptable, dsBaseName);
				// 若不存在则创建中间表
				if (!newFlage) {
					terminalDao.creatNewTable(temptable, dsBaseName);
				} else {
					// 查询上月份的数据是否已经存在
					boolean flage = terminalDao.queryMonDataExist(previousMon, dsBaseName);
					// 若上月份的数据已经存在中间表中，则把该月份的数据复制并把这些数据的testdate更新成本月的时间
					// 不存在不做处理
					if (flage) {
						boolean fas = terminalDao.queryMonDataExist(tdate, dsBaseName);
						if (fas) {
							terminalDao.delToMonData(tdate, dsBaseName);
						}
						int count = terminalDao.queryCount(dsBaseName);
						terminalDao.copyAndUpdate(previousMon, tdate, dsBaseName, count);
					}
				}

				// 遍历配置中表标识及对应字段值
				for (int k = 0; k < pointertypes.length; k++) {
					String pointertype = pointertypes[k];
					String types[] = pattern.split(pointertype);
					// 从遍历出来的值中把表标识及字段对应值分离出来
					String type = types[0];// 表标识
					// 现在获取到了code、时间、表标识，所以可以通过查询表对应表查询表名,这是数据源表
					String srctablename = terminalDao.getsrctablename(code, tdate, type);
					// 应先查询该表是否存在
					// 获取查询数据
					if (tdate.isEmpty()) {
						Calendar calendar = Calendar.getInstance();// 获取系统当前时间
						String month = new SimpleDateFormat("yyyyMM").format(calendar.getTime());
						tdate = month;
					}
					int start = 0;
					int end = 50000;
					boolean fag = true;
					while (fag) {
						// 分页查询数据
						listMap = terminalDao.getDataSrc(type, tdate, srctablename, dataBasename, start, end);
						// 进行数据的查询对比，拼接sql存入list中
						List<String> sqlList = terminalDao.dealGridData(listMap, tdate, dsBaseName);

						if (sqlList.size() > 0) {
							// 处理sql
							dataServiceImpl.insertList(sqlList, dsBaseName);
						}

						start += 50000;
						fag = TerminalDao.fag;
					}
				}
			}
		}
		// ----------------------------------处理中间表的数据---------------------------------------------------------
		// 在中间表数据完全生成后进行再进行数据的网格化处理
		terminalDao.queryAndDealMiddleData();

		enddate = new Date().getTime();
		System.out.println((startdate / 1000) + " ：开始时间  ：： 结束时间：  " + (enddate / 1000));
		System.out.println(startdate + " :开始时间: " + enddate + " :结束时间: " + " :用时: " + (enddate - startdate));
	}
}
