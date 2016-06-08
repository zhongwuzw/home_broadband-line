package com.opencassandra.monthterminalandhour.datastaticjson;

import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.opencassandra.descfile.ConfParser;

/**
 * 数据汇总 从中间表中把数据整理转入json表
 * 
 * @author：kxc
 * @date：Dec 15, 2015
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class DataStaticJsonData {

	// 配置code
	private static String[] codes = ConfParser.code;
	// 配置的month，hour标识（也相当于表标识）
	private static String[] intervals = ConfParser.interval;
	// 配置中的表标识
	private static String[] datatypes = ConfParser.datatype;
	// 配置时间
	// private static String tdates = ConfParser.testdate[0];
	
	private static String subdatabase = ConfParser.subdatabase;

	private static Pattern pattern1 = Pattern.compile("[,]");
	// 格式化信号强度及经纬度
	static Format fm = new DecimalFormat("#.##");
	// private static MonthTerminalAndHourDao mAndHourDao = new
	// MonthTerminalAndHourDao();

	private static Map dataMap = new HashMap();
	static Map uparmap = new HashMap();
	private static DataStaticJsonDao dJsonDao = new DataStaticJsonDao();

	public static void main(String[] args) {

		// 程序开始时间
		long startdate = 0;
		// 程序结束时间
		long enddate = 0;
		startdate = new Date().getTime();
		System.out.println("开始进行数据读取::::::  " + startdate);
		// 循环的code
		for (int i = 0; i < codes.length; i++) {
			String code = codes[i];

			String basename = subdatabase+"_" + code;
			// dJsonDao.getDatabaseName(code);
			// mAndHourDao.

			// 循环表标识
			for (int k = 0; k < datatypes.length; k++) {
				String datatype = datatypes[k];
				String tablename = "";
				String dsttablename = datatype + "_data_static_json";

				//查询数据要入的表是否存在
				boolean dstFlage = dJsonDao.queryDstTableExist(dsttablename, basename);

				if (dstFlage) {
					//存在则删除并创建该表
					dJsonDao.dropAndCreateDstTable(dsttablename, basename);
				} else {
					//不存在则创建该表
					dJsonDao.createDstTable(dsttablename, basename);
				}
				// 循环表时间标识
				for (int j = 0; j < intervals.length; j++) {
					
					String interval = intervals[j];
					
					//根据标识循环判断拼接数据源表名
					if (interval.contains("month") || interval.contains("terminal")) {
						tablename = datatype + "_month_terminal_static";
					} else {
						tablename = datatype + "_" + interval + "_static";
					}
					
					//分页取出数据
					int start = 0;
					int end = 20000;
					boolean flage = true;
					while(flage){
						
						//根据表名及限定条件查询数据
						List<Map> listMap = dJsonDao.getdata(tablename, basename, interval,start,end);
						//查询最大的月份
						String maxMonth = dJsonDao.getMaxMonth(tablename, basename);
						
						//处理已查询出来的数据
						TerminalData(listMap, maxMonth, tablename, basename, interval);
						flage = DataStaticJsonDao.fal;
						start +=20000;
						listMap.clear();
					}
					

					DataStaticJsonData data = new DataStaticJsonData();
					//对已处理过的数据进行拼接
					data.jiontData(dsttablename, interval, basename, tablename, datatype);
					dataMap.clear();
					uparmap.clear();
				}

			}

		}
		enddate = new Date().getTime();
		System.out.println("s数据读取完毕：：：：： " + enddate + " ::  " + new Date() + "  ::用时：：：  " + (enddate - startdate));
	}

	/**
	 * 
	 * 处理已查询出来的数据
	 * 
	 * @param listMap
	 * @param maxMonth
	 * @param tablename
	 * @param basename
	 * @param interval
	 * @return void
	 */
	public static void TerminalData(List<Map> listMap, String maxMonth, String tablename, String basename, String interval) {

		// 遍历查询取出的数据
		for (Iterator iterator = listMap.iterator(); iterator.hasNext();) {

			Map map = (Map) iterator.next();
			// 获取到终端或日期或时段
			String parameter = (String) map.get("parameter");
			parameter = "\"" + parameter + "\"";

			// 获取到测试次数
			String testtimes = (String) map.get("testtimes");
			// 数据源中的平均时延或平均速率
			String value = (String) map.get("value");
			// 数据源中的制式类型（2G、3G\4G等）
			String net_type = (String) map.get("net_type");

			// 数据源中的上级地区
			String uparea = (String) map.get("uparea");
			// 数据源中的地区值
			String area = (String) map.get("area");
			//
			String upareaand = map.get("uparea") + "," + map.get("area");

			// 把测试次数为空或0以及平均值为空的过滤
			if (testtimes == null || "0".equals(testtimes) || value == null || "".equals(value)||"".equals(net_type)||null==net_type) {
				continue;
			}

			// 以上下级地区为条件，终端的以测试次数排列，时段及日期的以yearmonth\hour为排序条件查询出对应的排序基准数据
			if (!uparmap.containsKey(upareaand)) {
				try {
					uparmap = dJsonDao.getTerMap(tablename, basename, uparea, area, interval);
				} catch (Exception e) {
					// TODO: handle exception
					continue;
				}
			}

			String ters = (String) uparmap.get(upareaand);
			// 把数据以&分割成数组
			String[] termin = ters.split("&");

			int terk = -1;// 定义变量保存指定元素的下标
			// 循环获取到相应的终端或日期或时段在该数组中的下标
			for (int i = 0; i < termin.length; i++) {
				if (termin[i].equals(parameter)) {
					terk = i;
				}
			}

			// 在全局的map中查找该对应的地区数据是否已存在
			if (dataMap.containsKey(upareaand)) {

				Map valueMap = (Map) dataMap.get(upareaand);

				Integer maptimes = (Integer) valueMap.get("testtimes");
				String mapvalue = (String) valueMap.get("value");
				double mapvalues = Double.parseDouble(mapvalue);
				// 不等-1说明，有该类型 等于-1则说明为其他数据，不在统计类型范围内则下标定位在数组最后
				if (terk == -1) {
					terk = termin.length;
				}

				// 该数据为各类型（各个终端或日期或时段）对应的总数及平均值
				int[] partaltimes = (int[]) valueMap.get("totalt");
				double[] partalvalues = (double[]) valueMap.get("totalv");
				int partimes = partaltimes[terk];
				double parvalus = partalvalues[terk];
				// 与对应下标数据进行计算
				int totaltimes = Integer.parseInt(testtimes) + partimes;
				double totalvalues = 0;
				// ping的时延与http及speed的平均速率计算方法不同
				if (tablename.contains("ping")) {
					if (parvalus == 0) {
						totalvalues = Double.parseDouble(value);
					} else {
						totalvalues = totaltimes / (Integer.parseInt(testtimes) / Double.parseDouble(value) + partimes / parvalus);

					}
				} else {
					totalvalues = (Integer.parseInt(testtimes) * Double.parseDouble(value) + partimes * parvalus) / totaltimes;
				}

				partaltimes[terk] = totaltimes;
				partalvalues[terk] = totalvalues;
				valueMap.put("totalt", partaltimes);
				valueMap.put("totalv", partalvalues);

				// 以上为计算总次数
				// ----------------------------G、3G等类型的计算----------------------------------------------

				if(!"unknow".equals(net_type)){
					
					int[] gtimes = (int[]) valueMap.get(net_type + "times");
					double[] gvalues = (double[]) valueMap.get(net_type + "values");
					
					
					int gtimev = (Integer) valueMap.get(net_type+"timev");
					double gvaluet = (Double) valueMap.get(net_type+"valuet");
					
					
					int ttimes = gtimes[terk];
					double tvlaue = gvalues[terk];
					int taltimes = Integer.parseInt(testtimes) + ttimes;
					
					double talvalues = 0;
					
					int gtaltimev = Integer.parseInt(testtimes) + gtimev;
					double gtotalvalue = 0;
					if (tablename.contains("ping")) {
						if (tvlaue == 0) {
							talvalues = Double.parseDouble(value);
						} else {
							talvalues = taltimes / (Integer.parseInt(testtimes) / Double.parseDouble(value) + ttimes / tvlaue);
						}
						if (gvaluet == 0) {
							gtotalvalue = Double.parseDouble(value);
						} else {
							gtotalvalue = gtaltimev / (Integer.parseInt(testtimes) / Double.parseDouble(value) + gtimev / gvaluet);
						}
						

					} else {
						talvalues = (Integer.parseInt(testtimes) * Double.parseDouble(value) + ttimes * tvlaue) / taltimes;
						gtotalvalue = (Integer.parseInt(testtimes) * Double.parseDouble(value) + gtimev * gvaluet) / gtaltimev;
					}
					
					gtimes[terk] = taltimes;
					gvalues[terk] = talvalues;
					
					valueMap.put(net_type + "times", gtimes);
					valueMap.put(net_type + "values", gvalues);
					
					valueMap.put(net_type+"timev", gtaltimev);
					valueMap.put(net_type+"valuet", gtotalvalue);
				}
				double totalvalue = 0;
				int totalt = Integer.parseInt(testtimes) + maptimes;
				if (tablename.contains("ping")) {
					totalvalue = totalt / (Integer.parseInt(testtimes) / Double.parseDouble(value) + maptimes / mapvalues);
				} else {
					totalvalue = (Integer.parseInt(testtimes) * Double.parseDouble(value) + maptimes * mapvalues) / totalt;
				}
				valueMap.put("testtimes", totalt);
				valueMap.put("value", fm.format(totalvalue));

				valueMap.put("maxmonth", maxMonth);
				dataMap.put(upareaand, valueMap);
			} else {// 该判断是为地区数据不存在全局的datamap中时

				Map infoMap = new HashMap();

				infoMap.put("net_type", net_type);

				infoMap.put("testtimes", Integer.parseInt(testtimes));
				infoMap.put("value", fm.format(Double.parseDouble(value)));
				infoMap.put("maxmonth", maxMonth);

				int arrayLenth = 0;

				// 如果是终端，则需加一个其他类型的判断所以数组的长度+1
				if (interval.equals("terminal")&&termin.length==9) {
					arrayLenth = termin.length + 1;
				} else {
					arrayLenth = termin.length;
				}

				System.out.println(arrayLenth+"  ::::");
				// 定义对应2G,3G,4G,WIFI 类型及总测试次数的数组
				int[] g2ttimes = new int[arrayLenth];
				double[] g2values = new double[arrayLenth];
				int[] g3ttimes = new int[arrayLenth];
				double[] g3values = new double[arrayLenth];
				int[] g4ttimes = new int[arrayLenth];
				double[] g4values = new double[arrayLenth];
				int[] wifittimes = new int[arrayLenth];
				double[] wifivalues = new double[arrayLenth];

				int[] totaltimes = new int[arrayLenth];
				double[] totalvalues = new double[arrayLenth];
				// -----------------------------------------------------

				int g2time = 0;
				double g2value = 0;
				int g3time = 0;
				double g3value = 0;
				
				int g4time = 0;
				double g4value = 0;
				int wifitime = 0;
				double wifivalue = 0;
				
				// 如果该数据不在类型中则是终端的数据（把该下标定位到最后一个）
				int terkCopy = 0;
				if (terk == -1) {
					terkCopy = termin.length;
				} else {
					terkCopy = terk;
				}

				if (net_type.equals("2G")) {
					g2ttimes[terkCopy] = Integer.parseInt(testtimes);
					g2values[terkCopy] = Double.parseDouble(value);
					g2time = Integer.parseInt(testtimes);
					g2value = Double.parseDouble(value);
				} else {
					g2ttimes[terkCopy] = 0;
					g2values[terkCopy] = 0;
					g2time = 0;
					g2value = 0;
				}
				if (net_type.equals("3G")) {
					g3ttimes[terkCopy] = Integer.parseInt(testtimes);
					g3values[terkCopy] = Double.parseDouble(value);
					g3time = Integer.parseInt(testtimes);
					g3value = Double.parseDouble(value);
				} else {
					g3ttimes[terkCopy] = 0;
					g3values[terkCopy] = 0;
					g3time = 0;
					g3value = 0;
				}
				if (net_type.equals("4G")) {
					g4ttimes[terkCopy] = Integer.parseInt(testtimes);
					g4values[terkCopy] = Double.parseDouble(value);
					g4time = Integer.parseInt(testtimes);
					g4value = Double.parseDouble(value);
				} else {
					g4ttimes[terkCopy] = 0;
					g4values[terkCopy] = 0;
					g4time = 0;
					g4value = 0;
				}
				if (net_type.equals("WIFI")) {
					wifittimes[terkCopy] = Integer.parseInt(testtimes);
					wifivalues[terkCopy] = Double.parseDouble(value);
					wifitime = Integer.parseInt(testtimes);
					wifivalue = Double.parseDouble(value);
				} else {
					wifittimes[terkCopy] = 0;
					wifivalues[terkCopy] = 0;
					wifitime = 0;
					wifivalue = 0;
				}
				totaltimes[terkCopy] = Integer.parseInt(testtimes);
				totalvalues[terkCopy] = Double.parseDouble(value);

				// map中放入相应数组，并把该map放入全局的datamap中
				infoMap.put("2Gtimes", g2ttimes);
				infoMap.put("2Gvalues", g2values);
				infoMap.put("3Gtimes", g3ttimes);
				infoMap.put("3Gvalues", g3values);
				infoMap.put("4Gtimes", g4ttimes);
				infoMap.put("4Gvalues", g4values);
				infoMap.put("WIFItimes", wifittimes);
				infoMap.put("WIFIvalues", wifivalues);
				infoMap.put("totalt", totaltimes);
				infoMap.put("totalv", totalvalues);
				
				
				infoMap.put("2Gtimev", g2time);
				infoMap.put("2Gvaluet", g2value);
				infoMap.put("3Gtimev", g3time);
				infoMap.put("3Gvaluet", g3value);
				infoMap.put("4Gtimev", g4time);
				infoMap.put("4Gvaluet", g4value);
				infoMap.put("WIFItimev", wifitime);
				infoMap.put("WIFIvaluet", wifivalue);
				
				dataMap.put(upareaand, infoMap);
			}

		}
	}

	/**
	 * 处理查询出的数据，拼接json和sql
	 * 
	 * @param dsttablename
	 * @param interval
	 * @param basename
	 * @return void
	 */
	public void jiontData(String dsttablename, String interval, String basename, String tablename, String datatype) {
		List<String> sqlList = new ArrayList<String>();
		// 遍历存放数据的datamap
		Iterator<Map.Entry> it = dataMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = it.next();
			String identifying = "";
			String distribute = "";
			String valuedistribute = "";

			// 根据测试类型判断是时延还是速率
			if (tablename.contains("ping")) {
				identifying = "平均时延";
			} else {
				identifying = "平均速率";
			}

			// 根据分组类型 确定 分布类型
			if (interval.contains("month")) {
				distribute = "测试次数月分布";
				if (datatype.contains("ping")) {
					valuedistribute = "平均Ping时延月分布";
				} else {
					valuedistribute = "平均" + datatype + "速率月分布";
				}
			} else if (interval.contains("terminal")) {
				distribute = "测试次数终端分布";
				if (datatype.contains("ping")) {
					valuedistribute = "平均Ping时延终端分布";
				} else {
					valuedistribute = "平均" + datatype + "速率终端分布";
				}
			} else if (interval.contains("hour")) {
				distribute = "测试次数24小时分布";
				if (datatype.contains("ping")) {
					valuedistribute = "平均Ping时延24小时分布";
				} else {
					valuedistribute = "平均" + datatype + "速率24小时分布";
				}
			}

			// 获取到存放在dataMap中的 数据存放的map
			Map valueMap = (Map) entry.getValue();
			// 该值为数据显示时的终端或日期或时段值
			String parv = (String) uparmap.get(entry.getKey());

			String[] parLength = parv.split("&");
			// 把数据替换成可显示的数据
			if (interval.equals("terminal")&&parLength.length==9) {
				parv = parv.replaceAll("&", ",") + "\"其他\"";
			} else {
				parv = parv.substring(0,parv.lastIndexOf("&"));
				parv = parv.replaceAll("&", ",");
			}

			// 获取最大的测试日期值
			String maxmonth = (String) valueMap.get("maxmonth");
			String yearmonth = maxmonth.substring(0, 4) + "年" + maxmonth.substring(4, maxmonth.length()) + "月";

			// 为显示数据测试次数的标头     ,\"subtext\": \"截至" + yearmonth + "\"
			/*String timestitle = "{\"title\" : {\"subtext\": \"" +distribute+"\" },\"tooltip\" : {\"trigger\": \"axis\"},\"legend\": {"
					+ " \"data\":[\"总测试次数\",\"2G\",\"3G\",\"4G\",\"WIFI\"]},\"toolbox\": { \"show\" : false,\"feature\" : { \"mark\" : {\"show\": true},\"dataView \": {\"show\": true, \"readOnly\": false},"
					+ "\"magicType\" : {\"show\": true, \"type\": [\"line\", \"bar\", \"stack\", \"tiled\"]},\"restore\" : {\"show\": true},\"saveAsImage\" : {\"show\": true}}},\"calculable\" : true,"
					+ " \"xAxis\" : [{\"type\" : \"category\",\"boundaryGap\" : false,\"data\" : [" + parv + "]}],\"yAxis\" : [" + "{" + "\"type\" : \"value\"}" + "],\"series\" : [";
			// 显示数据测试平均时延/速率 的标头  ,\"subtext\": \"截至" + valueMap.get("maxmonth") + "\"
			String valuestitle = "{\"title\" : {\"subtext\": \"" +valuedistribute+"\"},\"tooltip\" : {\"trigger\": \"axis\"},\"legend\": {"
					+ " \"data\":[\"" + identifying + "\",\"2G\",\"3G\",\"4G\",\"WIFI\"]},\"toolbox\": { \"show\" : false,\"feature\" : { \"mark\" : {\"show\": true},\"dataView \": {\"show\": true, \"readOnly\": false},"
					+ "\"magicType\" : {\"show\": true, \"type\": [\"line\", \"bar\", \"stack\", \"tiled\"]},\"restore\" : {\"show\": true},\"saveAsImage\" : {\"show\": true}}},\"calculable\" : true,"
					+ " \"xAxis\" : [{\"type\" : \"category\",\"boundaryGap\" : false,\"data\" : [" + parv + "]" + "}" + "],\"yAxis\" : [" + "{" + "\"type\" : \"value\"}" + "],\"series\" : [";*/
			String timestitle = "{\"title\" : {\"text\": \"" +distribute+"\",\"x\" : \"center\" ,\"textStyle\":{\"fontSize\":15}},\"tooltip\" : {\"trigger\": \"axis\"},\"legend\": {"
					+ " \"data\":[\"总测试次数\",\"2G\",\"3G\",\"4G\",\"WIFI\"],\"y\" : 35,\"textStyle\":{\"fontSize\":1},\"x\":\"left\"},\"toolbox\": { \"show\" : false,\"feature\" : { \"mark\" : {\"show\": true},\"dataView \": {\"show\": true, \"readOnly\": false},"
					+ "\"magicType\" : {\"show\": true, \"type\": [\"line\", \"bar\", \"stack\", \"tiled\"]},\"restore\" : {\"show\": true},\"saveAsImage\" : {\"show\": true}}},\"calculable\" : true,"
					+ " \"xAxis\" : [{\"type\" : \"category\",\"boundaryGap\" : false,\"data\" : [" + parv + "]}],\"yAxis\" : [" + "{" + "\"type\" : \"value\"}" + "],\"series\" : [";
			// 显示数据测试平均时延/速率 的标头  ,\"subtext\": \"截至" + valueMap.get("maxmonth") + "\"
			String valuestitle = "{\"title\" : {\"text\": \"" +valuedistribute+"\",\"x\" : \"center\",\"textStyle\":{\"fontSize\":15}},\"tooltip\" : {\"trigger\": \"axis\"},\"legend\": {"
					+ " \"data\":[\"" + identifying + "\",\"2G\",\"3G\",\"4G\",\"WIFI\"],\"y\" : 35,\"textStyle\":{\"fontSize\":1},\"x\":\"left\"},\"toolbox\": { \"show\" : false,\"feature\" : { \"mark\" : {\"show\": true},\"dataView \": {\"show\": true, \"readOnly\": false},"
					+ "\"magicType\" : {\"show\": true, \"type\": [\"line\", \"bar\", \"stack\", \"tiled\"]},\"restore\" : {\"show\": true},\"saveAsImage\" : {\"show\": true}}},\"calculable\" : true,"
					+ " \"xAxis\" : [{\"type\" : \"category\",\"boundaryGap\" : false,\"data\" : [" + parv + "]" + "}" + "],\"yAxis\" : [" + "{" + "\"type\" : \"value\"}" + "],\"series\" : [";
			/*String valuestitle = "option = {" + "title : {" + "text: '" + valuedistribute + "'," + "subtext: '截至" + valueMap.get("maxmonth") + "'" + "},tooltip : {" + "trigger: 'axis'" + "},"
					+ "legend: {" + " data:['" + identifying + "','2G','3G','4G','WIFI']" + "}," + " toolbox: {" + " show : false," + "feature : {" + "mark : {show: true},"
					+ " dataView : {show: true, readOnly: false}," + "magicType : {show: true, type: ['line', 'bar', 'stack', 'tiled']}," + "restore : {show: true}," + " saveAsImage : {show: true}"
					+ " }" + "}," + " calculable : true," + " xAxis : [" + "{" + " type : 'category'," + "boundaryGap : false," + " data : [" + parv + "]" + "}" + "]," + "yAxis : [" + "{"
					+ "type : 'value'" + "}" + "],series : [";*/
			String stack = "";
			String stackvlaue = "";
			String partotaltimes = "";
			String partotalvalues = "";

			// 遍历存放数据的map
			Iterator<Map.Entry> value = valueMap.entrySet().iterator();
			while (value.hasNext()) {
				Map.Entry values = value.next();
				String key = (String) values.getKey();

				// 该值为总平均值
				if (key.contains("totalv")) {
					double[] gvlaues = (double[]) values.getValue();
					for (int i = 0; i < gvlaues.length; i++) {
						double dvalues = gvlaues[i];
						partotalvalues += fm.format(dvalues) + ",";
					}
				}
				// 该值为总测试次数
				if (key.contains("totalt")) {
					int[] gtimes = (int[]) values.getValue();
					for (int i = 0; i < gtimes.length; i++) {
						int dtimes = gtimes[i];
						partotaltimes += dtimes + ",";
					}
				}

				// 该值为2G,3G,4G等的在该终端或日期或时段下的类型测试次数
				if (key.contains("Gtimes")) {
					int[] gtimes = (int[]) values.getValue();
					String times = "";
					for (int i = 0; i < gtimes.length; i++) {
						int dtimes = gtimes[i];
						times += dtimes + ",";
					}
					times = times.substring(0, times.lastIndexOf(","));
					stack += " {\"name\":\"" + key.substring(0, 2) + "\",\"type\":\"line\",\"data\":[" + times + "] },";
				}

				// 该值为WIFI的在该终端或日期或时段下的类型测试次数
				if (key.contains("WIFItimes")) {
					int[] gtimes = (int[]) values.getValue();
					String times = "";
					for (int i = 0; i < gtimes.length; i++) {
						int dtimes = gtimes[i];
						times += dtimes + ",";
					}
					times = times.substring(0, times.lastIndexOf(","));
					stack += " {\"name\":\"" + key.substring(0, 4) + "\",\"type\":\"line\",\"data\":[" + times + "] },";
				}

				// 该值为2G,3G,4G等的在该终端或日期或时段下的类型平均时延/速率
				if (key.contains("Gvalues")) {
					double[] gvlaues = (double[]) values.getValue();
					String mvalue = "";
					for (int i = 0; i < gvlaues.length; i++) {
						double dvalues = gvlaues[i];
						mvalue += fm.format(dvalues) + ",";
					}
					mvalue = mvalue.substring(0, mvalue.lastIndexOf(","));
					stackvlaue += " {\"name\":\"" + key.substring(0, 2) + "\",\"type\":\"line\",\"data\":[" + mvalue + "] },";
				}

				// 该值为WIFI的在该终端或日期或时段下的类型平均时延/速率
				if (key.contains("WIFIvalues")) {
					double[] gvlaues = (double[]) values.getValue();
					String mvalue = "";
					for (int i = 0; i < gvlaues.length; i++) {
						double dvalues = gvlaues[i];
						mvalue += fm.format(dvalues) + ",";
					}
					mvalue = mvalue.substring(0, mvalue.lastIndexOf(","));
					stackvlaue += " {\"name\":\"" + key.substring(0, 4) + "\",\"type\":\"line\",\"data\":[" + mvalue + "] },";
				}
			}

			// 把字符串最后的，去掉
			partotaltimes = partotaltimes.substring(0, partotaltimes.lastIndexOf(","));
			partotalvalues = partotalvalues.substring(0, partotalvalues.lastIndexOf(","));

			// 显示数据的尾字符串
			String timesover = "{\"name\":\"总测试次数\",\"type\":\"line\",\"data\":[" + partotaltimes + "] }]}";
			String valuessover = "{ \"name\":\"" + identifying + "\",\"type\":\"line\",\"data\":[" + partotalvalues + "] }]}";

			String timesjson = timestitle + stack + timesover;
			String valuejson = valuestitle + stackvlaue + valuessover;

			// 获取上下级的地区值，做为查询条件
			String areas = (String) entry.getKey();
			String[] upandarea = pattern1.split(areas);
			String uparea = upandarea[0];
			String area = upandarea[1];

			int totaltimes = (Integer) valueMap.get("testtimes");
			String tvalue = (String) valueMap.get("value");
			double totalvalue = Double.parseDouble(tvalue);
			String sql = "";
			

			int g2timev = (Integer) valueMap.get("2Gtimev");
			double g2valuet = (Double) valueMap.get("2Gvaluet");
			
			int g3timev = (Integer) valueMap.get("3Gtimev");
			double g3valuet = (Double) valueMap.get("3Gvaluet");
			
			int g4timev = (Integer) valueMap.get("4Gtimev");
			double g4valuet = (Double) valueMap.get("4Gvaluet");
			
			int wifitimev = (Integer) valueMap.get("WIFItimev");
			double wifivaluet = (Double) valueMap.get("WIFIvaluet");
			
			
			// 解决单引号问题
			timesjson = timesjson.replaceAll("'", "''");
			valuejson = valuejson.replaceAll("'", "''");
			boolean flage = dJsonDao.queryDataExist(uparea, area, dsttablename, basename);

			if (interval.contains("month")) {
				interval = "month";
			}

			if (flage) {
				sql = "update  " + dsttablename + " set " + interval + "_testtimes= '" + timesjson + "' ," + interval + "_value = '" + valuejson + "'  where uparea = '" + uparea + "' and area = '"
						+ area + "'";
			} else {
				sql = "insert into " + dsttablename + " (uparea,area," + interval + "_testtimes," + interval + "_value,testtimes,value,2gtvalue,3gtvalue,4gtvalue,wifitvalue,2gvaluet,3gvaluet,4gvaluet,wifivaluet) " + "values('" + uparea + "','" + area + "','" + timesjson
						+ "','" + valuejson + "','" + totaltimes + "','" + fm.format(totalvalue) + "','"+g2timev+"','"+g3timev+"','"+g4timev+"','"+wifitimev+"','"+fm.format(g2valuet)+"','"+fm.format(g3valuet)+"','"+fm.format(g4valuet)+"',"+fm.format(wifivaluet)+")";
			}
			sqlList.add(sql);
			System.out.println(sql+"  拼接sql");
			if (sqlList.size() % 20000 == 0) {
				dJsonDao.insertList(sqlList, basename);
				sqlList.clear();
			}
		}
		dJsonDao.insertList(sqlList, basename);
	}

}
