package com.opencassandra.descfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.portlet.ActionRequest;
import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.cassandra.thrift.Cassandra.AsyncProcessor.describe_cluster_name;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpRequest;

import sun.awt.OSInfo.OSType;

import com.csvreader.CsvReader;
import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import com.opencassandra.v01.dao.impl.InsertMysqlByDataToGPS;
import com.opencassandra.v01.dao.impl.InsertMysqlDao;
import com.sun.corba.se.spi.orb.StringPair;

public class DescFile {
	String root = "c:\\Orgnize\\";
	boolean isOverwrite = true;
	boolean isChange = false;
	static Map map = new HashMap();
	Map timeMap = new HashMap();
	static int mapCount = 0;
	static int length = 0;
	static List list_str = new ArrayList();
	static List list_datetime = new ArrayList();
//	private static Cluster cluster = HFactory.getOrCreateCluster(
//			ConfParser.cassandraServerName, ConfParser.cassandraServerIp+":"+ConfParser.cassandraServerPort);
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm",
			"ss", "SSS" };
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private static String ak = ConfParser.ak;
	int counts = 0;
	int errors = 0;
	public static int count = 0;
	private InsertMysqlDao insertMysqlDao = new InsertMysqlDao();
	private List appid = new ArrayList();
	private List sqlList = new ArrayList();
	private static String pathType = ConfParser.pathType;
	
	public static int getCount() {
		return count;
	}

	synchronized public static void setCount() {
		DescFile.count++;
	}

	public static int dataIndex = 256146;
	public static int successCount = 0;
	public static int failCount = 0;
	Statement statement;
	Connection conn;
	private ExecutorService analyzeThreadPool;

	static String srcPath = "\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.].*";

	static String srcPathPattern = "ORGKEY/SERVICETYPE/TERMINALTYPE/TERMINALMODEL/IMEI";
	// static String destPathPattern = "ORGKEY/YEAR_MONTH/DAY";
	static String destPathPattern = "ORGKEY/SERVICETYPE/TERMINALTYPE/TERMINALMODEL/IMEI";
	// static String fileNamePattern =
	// "TASKID_SERVICEID.CASEID_EXCUID-YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_MILLSECOND.DATATYPE.FILETYPE";
	static String fileNamePattern = "TASKID_SERVICEID.CASEID_EXCUID_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_MILLSECOND.DATATYPE.FILETYPE_SERVICETYPE_TERMINALTYPE_TERMINALMODEL_MAC_IMEI";

	static String fileNamePattern1 = "TASKID_SERVICEID.CASEID_EXCUID_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_MILLSECOND_PROCALTYPE.DATATYPE.FILETYPE_SERVICETYPE_TERMINALTYPE_TERMINALMODEL_MAC_IMEI";
	// 00000000_01001.000_00000000_2014_03_21_09_05_03-HTTP.abstract.csv_speedtest_Mobile_HUAWEI
	// D2-6070_EMPTY_862750020060908
	static String fileNamePattern2 = "TASKID_SERVICEID.CASEID_EXCUID_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_PROCALTYPE.DATATYPE.FILETYPE_SERVICETYPE_TERMINALTYPE_TERMINALMODEL_MAC_IMEI";

	static String fileNamePattern3 = "TASKID_SERVICEID.CASEID_EXCUID_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND-PROCALTYPE.DATATYPE.FILETYPE";
	// _TCP/HTTP .abstract/detail.csv
	static String fileNamePattern4 = "TASKID_SERVICEID.CASEID_EXCUID_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_MILLSECOND-PROCALTYPE.DATATYPE.FILETYPE";
	// _TCP/HTTP .abstract/detail.csv
	static String fileNamePattern5 = "TASKID_SERVICEID.CASEID_EXCUID_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_MILLSECOND.DATATYPE.FILETYPE";

	// .abstract/detail.csv

	static String regexName = "[^_.]+";
	// static String regexName = "[^_.-]+";
	static String regexPath = "[^/\\\\]+";

	static public Map<String, String> patternSpilt(String fileNamePattern,
			String fileName, String regexName, String srcPathPattern,
			String srcPathName, String regexPath) {

		Map<String, String> result = new HashMap<String, String>();

		// 将TASKID_SERVICEID.CASEID_EXCUID-YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_MILLSECOND.DATATYPE.FILETYPE_SERVICETYPE_TERMINALTYPE_TERMINALMODEL_MAC_IMEI格式的数据
		// 修改成TASKID_SERVICEID.CASEID_EXCUID-YEAR_MONTH_DAY_HOUR_MINUTE_SECOND_MILLSECOND.DATATYPE.FILETYPE格式的文件名
		String realFileName = fileName;// .substring(0,
										// fileName.indexOf("-_-"));
		result.put("realfilename", realFileName);
		String orgKey = "4";
		// AbsBodyInsertDao.queryByFilename(realFileName);
		if (orgKey != null) {
			result.put("ORGKEY", orgKey);
		} else {
			result.put("ORGKEY", "UNKNOW");
		}

		if (fileName != null) {
			fileName = fileName.replaceAll("-_-", "_");
			fileName = fileName.replaceFirst("-", "_");
		}

		Pattern pattern = Pattern.compile(regexName);
		Matcher matcherName = pattern.matcher(fileName);
		Matcher matcherPattern = pattern.matcher(fileNamePattern);

		boolean isName = false;
		boolean isPattern = false;

		while (true) {
			isName = matcherName.find();
			isPattern = matcherPattern.find();
			if (isName && isPattern) {
				// System.out.println(matcherName.group()+"-----"+matcherPattern.group());
				System.out.println(matcherPattern.group() + ","
						+ matcherName.group());
				result.put(matcherPattern.group(), matcherName.group());
			} else {
				break;
			}
		}
		if (!(isName ^ isPattern)) {
			/*
			 * File srcFile = (new File(srcPathName)).getParentFile();
			 * 
			 * Pattern patternPath = Pattern.compile(regexPath); Matcher
			 * matcherSrcPathPattern = patternPath.matcher(srcPathPattern);
			 * 
			 * try { List<String> strs = new ArrayList<String>(); while
			 * (matcherSrcPathPattern.find()) {
			 * strs.add(matcherSrcPathPattern.group()); } if (strs.size() > 0) {
			 * for (int i = strs.size() - 1; i >= 0; i--) {
			 * result.put(strs.get(i), srcFile.getName()); srcFile =
			 * srcFile.getParentFile(); } } } catch (Exception ex) {
			 * ex.printStackTrace(); System.out.println("文件路径有误: " +
			 * srcPathName); return null; }
			 */
			return result;
		} else {
			System.out.println("文件名称有误: " + fileName);
			return null;
		}
	}

	static public Map<String, String> patternSpilt(String fileName,
			String srcPathName) {
		/*
		 * 1: TASKID _SERVICEID .CASEID _EXCUID -YEAR _MONTH _DAY _HOUR _MINUTE
		 * _SECOND -PROCALTYPE .DATATYPE .FILETYPE 00000000 _01001 .000
		 * _00000000 -1970 _01 _02 _09 _24 _59 -TCP .abstract .csv 2: TASKID
		 * _SERVICEID .CASEID _EXCUID -YEAR _MONTH _DAY _HOUR _MINUTE _SECOND
		 * _MILLSECOND -PROCALTYPE .DATATYPE .FILETYPE 00000000 _01001 .000
		 * _00000000 -2013 _04 _04 _09 _39 _58 _074 -TCP .detail .csv 4: TASKID
		 * _SERVICEID .CASEID _EXCUID -YEAR _MONTH _DAY _HOUR _MINUTE _SECOND
		 * _MILLSECOND .DATATYPE .FILETYPE 00000000 _01001 .000 _0 -2014 _09 _16
		 * _15 _21 _56 _559 .abstract .csv 5: 1378892394879665_01001.927_1543
		 * -2013_09_11 _17 _40_15 _796 .abstract.csv 3:
		 * 00000000_01001.000_00000000 -2014_02_12 _0017 _50_10
		 * -TCP.abstract.csv
		 */
		if ((fileName.contains("-TCP") || fileName.contains("－HTTP") || fileName
				.contains("-UDP"))
				&& (fileName.contains("－2") || fileName.contains("－1"))) {
			return patternSpilt(fileNamePattern1, fileName, regexName,
					srcPathPattern, srcPathName, regexPath);
		} else if (fileName.contains("-TCP") || fileName.contains("－HTTP")
				|| fileName.contains("-UDP") && fileName.split("_").length == 8) {
			System.out.println("******************************************4");
			return patternSpilt(fileNamePattern4, fileName, regexName,
					srcPathPattern, srcPathName, regexPath);
		} else if (fileName.split("_").length == 8) {
			System.out.println("******************************************3");
			return patternSpilt(fileNamePattern3, fileName, regexName,
					srcPathPattern, srcPathName, regexPath);
		} else if (fileName.contains("-TCP") || fileName.contains("－HTTP")
				|| fileName.contains("-UDP")) {
			return patternSpilt(fileNamePattern2, fileName, regexName,
					srcPathPattern, srcPathName, regexPath);
		} else if (fileName.split("_").length == 9) {
			System.out.println("******************************************5");
			return patternSpilt(fileNamePattern5, fileName, regexName,
					srcPathPattern, srcPathName, regexPath);
		} else {
			return patternSpilt(fileNamePattern, fileName, regexName,
					srcPathPattern, srcPathName, regexPath);
		}
	}

	static public List<String> destPathSpilt(String destPathPattern) {
		if (destPathPattern != null && !"".equals(destPathPattern)) {
			List<String> destPath = new ArrayList<String>();
			Pattern pattern = Pattern.compile(regexPath);
			Matcher matcherName = pattern.matcher(destPathPattern);
			while (matcherName.find()) {
				destPath.add(matcherName.group());

			}
			return destPath;
		}
		return null;
	}

	static public List<String> destPathSpilt() {
		return destPathSpilt(destPathPattern);
	}

	static public Map<String, String> fileNameSpilt(String fileNamePattern,
			String fileName, String regexName) {
		Map<String, String> result = new HashMap<String, String>();
		Pattern pattern = Pattern.compile(regexName);
		Matcher matcherName = pattern.matcher(fileName);
		Matcher matcherPattern = pattern.matcher(fileNamePattern);

		boolean isName = false;
		boolean isPattern = false;

		while (true) {
			isName = matcherName.find();
			isPattern = matcherPattern.find();
			if (isName && isPattern) {
				// System.out.println(matcherName.group()+"-----"+matcherPattern.group());
				result.put(matcherPattern.group(), matcherName.group());
			} else {
				break;
			}
		}
		if (!(isName ^ isPattern)) {
			return result;
		} else {
			System.out.println("文件名称有误: " + fileName);
			return null;
		}
	}

	public ExecutorService getAnalyzeThreadPool() {
		return analyzeThreadPool;
	}

	public void setAnalyzeThreadPool(ExecutorService analyzeThreadPool) {
		this.analyzeThreadPool = analyzeThreadPool;
	}

	public String getRoot() {
		return root;
	}

	public void setRoot(String root) {
		this.root = root;
	}

	public boolean isOverwrite() {
		return isOverwrite;
	}

	public void setOverwrite(boolean isOverwrite) {
		this.isOverwrite = isOverwrite;
	}

	public void setCounts(int counts) {
		this.counts = counts;
	}

	public int getCounts() {
		return counts;
	}

	public boolean isChange() {
		return isChange;
	}

	public void setChange(boolean isChange) {
		this.isChange = isChange;
	}

	public DescFile() {
		super();
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	public DescFile(String root) {
		super();
		this.root = root;
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	/**
	 * @override
	 */
	public void run() {
		System.out.println("counts: " + this.getCounts());
	}

	/**
	 * 读取CSV文件
	 */
	public ArrayList<String[]> readCsv(File filePath) {
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath.getAbsolutePath();
			CsvReader reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				String[] values = null;
				values = reader.getValues();
				csvList.add(values);
			}
			reader.close();

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {

		}
		return csvList;
	}

	/**
	 * 判断CSV文件中某一行是不是空行
	 * 
	 * @param values
	 * @return
	 */
	public boolean isBlankLine(String[] values) {
		boolean isBlank = true;
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				if (values[i] != null && !"".equals(values[i])) {
					isBlank = false;
				}
			}
		}
		return isBlank;
	}

	/**
	 * 将给定GPS信息，格式116°20′41.3″E，转换为XX.xxx
	 * 
	 * @param sourceData
	 */
	public static String tranferGps(String sourceData) {
		if (sourceData != null && sourceData.contains("°")
				&& sourceData.contains("′") && sourceData.contains("″")) {

			// 将输入的字符串分成两截，分别为度数du_string，分数fen_string
			String du_string = sourceData.substring(0, sourceData.indexOf("°"));
			// substring方法两个参数遵循规则"前包含后不包含"，例，”1234“.substring(0,2)返回的结果是index为0到1的子串，即"12"
			String fen_string = sourceData.substring(
					sourceData.indexOf("°") + 1, sourceData.indexOf("′"));
			// substring方法两个参数遵循规则"前包含后不包含"，例，”1234“.substring(0,2)返回的结果是index为0到1的子串，即"12"
			String miao_string = sourceData.substring(
					sourceData.indexOf("′") + 1, sourceData.indexOf("″"));

			double du = Double.parseDouble(du_string);
			double fen = Double.parseDouble(fen_string);
			double miao = Double.parseDouble(miao_string);

			double result = du + fen / 60 + miao / 3600;
			DecimalFormat df1 = new DecimalFormat("0.0000000");
			String resultStr = df1.format(result);
			return resultStr;
		}
		return "";
	}

	/**
	 * 读取DeviceInfo.CSV文件
	 */
	public Map<String, String> splitDeviceInfoCsv2(
			Map<String, String> fileAttribute, File filePath) {
		ArrayList<String[]> deviceInfoTerminal = new ArrayList<String[]>();
		ArrayList<String[]> deviceInfoNetwork = new ArrayList<String[]>();
		ArrayList<String[]> deviceInfoTestinfo = new ArrayList<String[]>();
		ArrayList<String[]> csvList = this.readCsv(filePath);
		int dataEreaIndex = 0;
		for (int i = 0; i < csvList.size(); i++) {
			if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
				if (csvList.get(i)[0] != null) {
					if (csvList.get(i)[0].equals("运营商")) {
						dataEreaIndex = 1;
					} else if (csvList.get(i)[0].equals("测试位置")) {
						dataEreaIndex = 2;
					}
				}
				if (dataEreaIndex == 0) {
					deviceInfoTerminal.add(csvList.get(i));
				} else if (dataEreaIndex == 1) {
					deviceInfoNetwork.add(csvList.get(i));
				} else if (dataEreaIndex == 2) {
					deviceInfoTestinfo.add(csvList.get(i));
				}
			} else {
				dataEreaIndex++;
			}
		}
		fileAttribute.put(
				"mfr",
				deviceInfoTerminal.get(5)[1] != null ? deviceInfoTerminal
						.get(5)[1] : "");
		fileAttribute
				.put("sim_state",
						deviceInfoNetwork.get(2)[1] != null ? deviceInfoNetwork
								.get(2)[1] : "");
		fileAttribute
				.put("net_state",
						deviceInfoNetwork.get(4)[1] != null ? deviceInfoNetwork
								.get(4)[1] : "");
		fileAttribute
				.put("operator",
						deviceInfoNetwork.get(0)[1] != null ? deviceInfoNetwork
								.get(0)[1] : "");
		fileAttribute
				.put("roaming",
						deviceInfoNetwork.get(5)[1] != null ? deviceInfoNetwork
								.get(5)[1] : "");
		fileAttribute.put(
				"inside_ip",
				deviceInfoTestinfo.get(4)[1] != null ? deviceInfoTestinfo
						.get(4)[1] : "");
		fileAttribute.put(
				"outside_ip",
				deviceInfoTestinfo.get(5)[1] != null ? deviceInfoTestinfo
						.get(5)[1] : "");
		fileAttribute.put(
				"mac",
				deviceInfoTestinfo.get(6)[1] != null ? deviceInfoTestinfo
						.get(6)[1] : "");
		fileAttribute.put(
				"l_net_type",
				deviceInfoTestinfo.get(1)[1] != null ? deviceInfoTestinfo
						.get(1)[1] : "");
		fileAttribute.put(
				"l_net_name",
				deviceInfoTestinfo.get(2)[1] != null ? deviceInfoTestinfo
						.get(2)[1] : "");
		fileAttribute.put(
				"os_ver",
				deviceInfoTerminal.get(3)[1] != null ? deviceInfoTerminal
						.get(3)[1] : "");
		fileAttribute.put(
				"ots_ver",
				deviceInfoTerminal.get(4)[1] != null ? deviceInfoTerminal
						.get(4)[1] : "");
		fileAttribute.put(
				"machine_model",
				deviceInfoTerminal.get(7)[1] != null ? deviceInfoTerminal
						.get(7)[1] : "");
		fileAttribute.put(
				"hw_ver",
				deviceInfoTerminal.get(10)[1] != null ? deviceInfoTerminal
						.get(10)[1] : "");
		fileAttribute.put(
				"ots_test_position",
				deviceInfoTestinfo.get(0)[1] != null ? deviceInfoTestinfo
						.get(0)[1] : "");
		fileAttribute.put(
				"server_info",
				deviceInfoTestinfo.get(3)[1] != null ? deviceInfoTestinfo
						.get(3)[1] : "");

		return fileAttribute;
	}

	public boolean changePath(File file, Map<String, String> fileAttributes) {
		if (!file.getAbsolutePath().contains(".abstract.csv")) {
			return false;
		}
		List<String> destPath = destPathSpilt();
		String destFilePath = "";
		if (destPath != null && destPath.size() > 0) {
			for (int i = 0; i < destPath.size(); i++) {
				if (!destPath.get(i).contains("_")) {
					destFilePath += fileAttributes.get(destPath.get(i))
							+ File.separator;
				} else {
					String[] strs = destPath.get(i).split("_");
					for (int j = 0; j < strs.length; j++) {
						destFilePath += fileAttributes.get(strs[j]) + "_";
					}
					destFilePath = destFilePath.substring(0,
							destFilePath.length() - 1)
							+ File.separator;
				}
			}
		}

		destFilePath = this.root + destFilePath;
		File destFile = new File(destFilePath
				+ fileAttributes.get("realfilename"));
		if (!destFile.exists()) {
			if (this.copyFile(file, destFile)) {
				System.out.println("成功生成目标文件: " + destFile.getAbsolutePath());
			} else {
				return false;
			}
		} else if (destFile.exists() && !isOverwrite) {
			System.out.println("目标文件已存在，未覆盖: " + destFile.getAbsolutePath());
			// return false;
		} else {
			file.renameTo(new File(file.getAbsoluteFile() + ".bak"));
			if (this.copyFile(file, destFile)) {
				System.out
						.println("目标文件已存在，已覆盖: " + destFile.getAbsolutePath());
				// return true;
			}
		}
		File deviceFile = new File(file.getAbsolutePath().replace(
				".abstract.csv", ".deviceInfo.csv"));
		if (deviceFile.exists()) {
			File destDeviceFile = new File(destFilePath
					+ fileAttributes.get("realfilename").replace(
							".abstract.csv", ".deviceInfo.csv"));
			if (!destDeviceFile.exists()) {
				if (this.copyFile(deviceFile, destDeviceFile)) {
					System.out.println("成功生成目标文件: "
							+ destDeviceFile.getAbsolutePath());
				} else {
					return false;
				}
			} else if (destDeviceFile.exists() && !isOverwrite) {
				System.out.println("目标文件已存在，未覆盖: "
						+ destDeviceFile.getAbsolutePath());
				// return false;
			} else {
				if (this.copyFile(deviceFile, destDeviceFile)) {
					System.out.println("目标文件已存在，已覆盖: "
							+ destDeviceFile.getAbsolutePath());
					// return true;
				}
				deviceFile.renameTo(new File(deviceFile.getAbsoluteFile()
						+ ".bak"));
			}
		}

		File monitorFile = new File(file.getAbsolutePath().replace(
				".abstract.csv", ".monitor.csv"));
		if (monitorFile.exists()) {
			File destMonitorFile = new File(destFilePath
					+ fileAttributes.get("realfilename").replace(
							".abstract.csv", ".monitor.csv"));
			if (!destMonitorFile.exists()) {
				if (this.copyFile(monitorFile, destMonitorFile)) {
					System.out.println("成功生成目标文件: "
							+ destMonitorFile.getAbsolutePath());
				} else {
					return false;
				}
			} else if (destMonitorFile.exists() && !isOverwrite) {
				System.out.println("目标文件已存在，未覆盖: "
						+ destMonitorFile.getAbsolutePath());
				// return false;
			} else {
				if (this.copyFile(monitorFile, destMonitorFile)) {
					System.out.println("目标文件已存在，已覆盖: "
							+ destMonitorFile.getAbsolutePath());
					// return true;
				}
				monitorFile.renameTo(new File(monitorFile.getAbsoluteFile()
						+ ".bak"));
			}
		}

		File detailFile = new File(file.getAbsolutePath().replace(
				".abstract.csv", ".detail.csv"));
		if (detailFile.exists()) {
			File destDetailFile = new File(destFilePath
					+ fileAttributes.get("realfilename").replace(
							".abstract.csv", ".detail.csv"));
			if (!destDetailFile.exists()) {
				if (this.copyFile(detailFile, destDetailFile)) {
					System.out.println("成功生成目标文件: "
							+ destDetailFile.getAbsolutePath());
					return true;
				}
			} else if (destDetailFile.exists() && !isOverwrite) {
				System.out.println("目标文件已存在，未覆盖: "
						+ destDetailFile.getAbsolutePath());
				return false;
			} else {
				if (this.copyFile(detailFile, destDetailFile)) {
					System.out.println("目标文件已存在，已覆盖: "
							+ destDetailFile.getAbsolutePath());
					return true;
				}
				detailFile.renameTo(new File(detailFile.getAbsoluteFile()
						+ ".bak"));
			}
		}
		return false;
	}

	static class MyFilter implements FilenameFilter {
		private ArrayList<String> type;
		private String must = "";

		public MyFilter(ArrayList<String> type, String must) {
			this.type = type;
			this.must = must;
		}

		@Override
		public boolean accept(File dir, String name) {
			boolean isBingo = false;
			for (int i = 0; i < type.size(); i++) {
				if (name.contains(type.get(i)) && name.contains(must)) {
					isBingo = true;
				}
			}
			return isBingo;
		}
	}

	public boolean copyFile(File srcFile, File destFile) {
		boolean isSuccess = false;
		InputStream inStream = null;
		FileOutputStream fs = null;
		try {
			if (!destFile.getParentFile().exists()) {
				destFile.getParentFile().mkdirs();
			}
			if (destFile.createNewFile()) {
				int byteread = 0;
				inStream = new FileInputStream(srcFile);
				fs = new FileOutputStream(destFile);
				byte[] buffer = new byte[4096];
				while ((byteread = inStream.read(buffer)) != -1) {
					fs.write(buffer, 0, byteread);
				}
				isSuccess = true;
			} else {
				System.out.println("目标文件创建失败: " + destFile.getAbsolutePath());
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("目标文件创建失败: " + destFile.getAbsolutePath());
			return false;
		} finally {
			try {
				if (inStream != null) {
					inStream.close();
				}
				if (fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try {
				if (isSuccess == true) {
					srcFile.delete();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return true;
	}

	static public void main(String[] args) {
		InsertMysqlDao insertMysqlDao = new InsertMysqlDao();
		DescFile desc = new DescFile();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> Start!");
		desc.setChange(true);
		desc.appid = desc.getAppids();
		String file[] =  new String[]{};
		file = ConfParser.srcReportPath;
		System.out.println("开始获取路径进行遍历"+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		String code1 = (ConfParser.code!=null&&ConfParser.code.length>0)?ConfParser.code[0]:"";
		System.out.println(ConfParser.url);
		String database = ConfParser.url.substring(ConfParser.url.lastIndexOf("/")+1,ConfParser.url.length());
		System.out.println(ConfParser.url);
		try {
			insertMysqlDao.start();	
		}catch (MySQLSyntaxErrorException e) {
			insertMysqlDao.createDB(database);
			try {
				insertMysqlDao.start();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			insertMysqlDao.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if("all".equals(code1)){
		}else{
			if(pathType.equals("default")){
				for (int i = 0; i < file.length; i++) {
					String filePath = file[i];
					System.out.println(filePath);
					String org = "";
						String filePathOrg = "";
						if(filePath.endsWith("Default")){
							filePathOrg = filePath.replace(File.separator+"Default", "");
						}
						if(filePathOrg.contains(File.separator)){
							org = filePathOrg.substring(filePathOrg.lastIndexOf(File.separator)+1, filePathOrg.length());
						}else{
							continue;
						}
					desc.findFile(filePath,org);
				}
			}else{
				for (int i = 0; i < file.length; i++) {
					String filePath = file[i];
					System.out.println(filePath);
					String org = "";
//						String filePathOrg = "";
//						if(filePath.endsWith("Default")){
//							filePathOrg = filePath.replace(File.separator+"Default", "");
//						}
//						if(filePathOrg.contains(File.separator)){
//							org = filePathOrg.substring(filePathOrg.lastIndexOf(File.separator)+1, filePathOrg.length());
//						}else{
//							continue;
//						}
					while(filePath.endsWith(File.separator)){
						filePath = filePath.substring(0, filePath.length()-1);
					}
					org = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.length());
					desc.findFile(filePath,org);
				}
			}
			
		}
		GetPostTest getPostTest = new GetPostTest();
		String codeUrl = ConfParser.getCodePath;
		if(codeUrl.isEmpty()){
			if(pathType.equals("default")){
				String[] paths = getPostTest.getPathsDefault(desc.appid);
				for (int i = paths.length - 1; i >= 0; i--) {
					String filePath = paths[i];
					if (filePath == null || filePath.isEmpty()) {
						continue;
					}
					System.out.println(filePath);
					String org = "";
					String filePathOrg = "";
					if (filePath.endsWith("Default")) {
						filePathOrg = filePath.replace(File.separator + "Default",
								"");
					}
					if (filePathOrg.contains(File.separator)) {
						org = filePathOrg.substring(filePathOrg
								.lastIndexOf(File.separator) + 1, filePathOrg
								.length());
					}
					desc.findFile(filePath, org);
				}
			}else{
				String[] paths = getPostTest.getPaths(desc.appid);
				for (int i = paths.length - 1; i >= 0; i--) {
					String filePath = paths[i];
					if (filePath == null || filePath.isEmpty()) {
						continue;
					}
					System.out.println(filePath);
					String org = "";
					while(filePath.endsWith(File.separator)){
						filePath = filePath.substring(0, filePath.length()-1);
					}
					org = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.length());
					desc.findFile(filePath, org);
				}
			}
			
		}else{
			if(pathType.equals("default")){
				/**
				 * 带Default的解析路径获取
				 */
				String dbName = "";
				for (int j = 0; j < ConfParser.code.length; j++) {
					ArrayList keyList = new ArrayList();
					String code2 = ConfParser.code[j];
					String codeStr = "code=" + code2;
					System.out.println(ConfParser.url);
					if(!ConfParser.url.endsWith("/")){
						System.out.println(ConfParser.url);
						if(j>0){
							if(ConfParser.url.contains("_"+ConfParser.code[j-1])){
								ConfParser.url = ConfParser.url.replace("_"+ConfParser.code[j-1], "");
							}
						}
						dbName = ConfParser.url.substring(ConfParser.url.lastIndexOf("/")+1,ConfParser.url.length());
						dbName = dbName + "_" + code2;
						ConfParser.url = ConfParser.url+"_"+code2;
						System.out.println(ConfParser.url);
					}else{
						System.out.println(ConfParser.url);
						dbName = "testdataanalyse";
						ConfParser.url = ConfParser.url + "testdataanalyse";
						System.out.println(ConfParser.url);
					}
					System.out.println(ConfParser.url);
					try {
						insertMysqlDao.start();	
					}catch (MySQLSyntaxErrorException e) {
						insertMysqlDao.createDB(dbName);
						try {
							insertMysqlDao.start();
						} catch (Exception e1) {
							e1.printStackTrace();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					try {
						insertMysqlDao.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(code2.equals("all")){
						ConfParser.csvwritepath = ConfParser.csvwritepath.substring(0,ConfParser.csvwritepath.lastIndexOf(File.separator)+1) +code2;
						String paths = "";
						if(ConfParser.org_prefix!=null && !ConfParser.org_prefix.isEmpty()){
							paths = ConfParser.org_prefix;
						}else {
							if(ConfParser.srcReportPath!=null && ConfParser.srcReportPath.length>0 && ConfParser.srcReportPath[0]!=null){
								paths = ConfParser.srcReportPath[0];
							}
						}
						File filePaths = new File(paths);
						File files[] = filePaths.listFiles(new FilenameFilter(){
				            public boolean accept(File dir, String name){
				            	if(dir.isDirectory()){
				            		return true;
				            	}
				            	return false;
				            }
				        }); 
						for (int i = 0; i < files.length; i++) {
							File rootFile = files[i];
							String rootFileOrg = rootFile.getAbsolutePath().replace(ConfParser.org_prefix, "");
							if(rootFileOrg.endsWith(File.separator)){
								rootFileOrg = rootFileOrg.substring(0,rootFileOrg.lastIndexOf(File.separator));
							}
							desc.findFile(rootFile.getAbsolutePath(), rootFileOrg);
						}
					}else{
						ConfParser.csvwritepath = ConfParser.csvwritepath.substring(0,ConfParser.csvwritepath.lastIndexOf(File.separator)+1) +code2;
						String sr = GetPostTest.sendGet(codeUrl, codeStr);
						System.out.println(sr);
						JSONObject json = JSONObject.fromObject(sr);
						try {
							JSONObject thisOrgInfo = json.getJSONObject("thisOrgInfo");
							String keySelf = thisOrgInfo.getString("key");
							String storePathSelf = thisOrgInfo.getString("storepath");
							if (keySelf == null) {
								keySelf = "";
							}
							if (!keySelf.isEmpty()) {
								keyList.add(keySelf);
							}
							if (storePathSelf != null && !storePathSelf.isEmpty()) {
								storePathSelf = storePathSelf.replace("/", File.separator)
										.replace("\\", File.separator);
								if (storePathSelf.endsWith("Default")) {
									storePathSelf = storePathSelf.replace(File.separator
											+ "Default", "");
								}
								keyList.add(storePathSelf);
							}
						} catch (Exception e) {
							e.printStackTrace();
							continue;
						}
						String[] paths = getPostTest.getPaths(desc.appid, keyList);
						for (int i = paths.length - 1; i >= 0; i--) {
							String filePath = paths[i];
							if (filePath == null || filePath.isEmpty()) {
								continue;
							}
							System.out.println(filePath);
							String org = "";
							String filePathOrg = "";
							if (filePath.endsWith("Default")) {
								filePathOrg = filePath.replace(File.separator + "Default",
										"");
							}
							if (filePathOrg.contains(File.separator)) {
								org = filePathOrg.substring(filePathOrg
										.lastIndexOf(File.separator) + 1, filePathOrg
										.length());
							}
//							org = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.length());
							desc.findFile(filePath, org);
						}
					}
					
				}
			}else{
				/**
				 * 不带Default的解析路径获取
				 */
				if(code1.equals("all")){
					String dbName = "";
					if(!ConfParser.url.endsWith("/")){
						System.out.println(ConfParser.url);
						dbName = ConfParser.url.substring(ConfParser.url.lastIndexOf("/")+1,ConfParser.url.length());
						dbName = dbName + "_" + code1;
						ConfParser.url = ConfParser.url+"_"+code1;
						System.out.println(ConfParser.url);
					}else{
						System.out.println(ConfParser.url);
						dbName = "testdataanalyse";
						ConfParser.url = ConfParser.url + "testdataanalyse";
						System.out.println(ConfParser.url);
					}
					System.out.println(ConfParser.url);
					try {
						insertMysqlDao.start();	
					}catch (MySQLSyntaxErrorException e) {
						insertMysqlDao.createDB(dbName);
						try {
							insertMysqlDao.start();
						} catch (Exception e1) {
							e1.printStackTrace();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					try {
						insertMysqlDao.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					ConfParser.csvwritepath = ConfParser.csvwritepath.substring(0,ConfParser.csvwritepath.lastIndexOf(File.separator)+1) + code1;
					String paths = "";
					if(ConfParser.org_prefix!=null && !ConfParser.org_prefix.isEmpty()){
						paths = ConfParser.org_prefix;
					}else {
						if(ConfParser.srcReportPath!=null && ConfParser.srcReportPath.length>0 && ConfParser.srcReportPath[0]!=null){
							paths = ConfParser.srcReportPath[0];
						}
					}
					File filePaths = new File(paths);
					File files[] = filePaths.listFiles(new FilenameFilter(){
			            public boolean accept(File dir, String name){
			            	if(dir.isDirectory()){
			            		return true;
			            	}
			            	return false;
			            }
			        }); 
					for (int i = 0; i < files.length; i++) {
						File rootFile = files[i];
						String rootFileOrg = rootFile.getAbsolutePath().replace(ConfParser.org_prefix, "");
						if(rootFileOrg.endsWith(File.separator)){
							rootFileOrg = rootFileOrg.substring(0,rootFileOrg.lastIndexOf(File.separator));
						}
						desc.findFile(rootFile.getAbsolutePath(), rootFileOrg);
					}
				}else{
					String dbName = "";
					for (int j = 0; j < ConfParser.code.length; j++) {
						ArrayList keyList = new ArrayList();
						String code2 = ConfParser.code[j];
						String codeStr = "code=" + code2;
						System.out.println(ConfParser.url);
						ConfParser.csvwritepath = ConfParser.csvwritepath.substring(0,ConfParser.csvwritepath.lastIndexOf(File.separator)+1) + code2;
						if(!ConfParser.url.endsWith("/")){
							System.out.println(ConfParser.url);
							if(j>0){
								if(ConfParser.url.contains("_"+ConfParser.code[j-1])){
									ConfParser.url = ConfParser.url.replace("_"+ConfParser.code[j-1], "");
								}	
							}
							dbName = ConfParser.url.substring(ConfParser.url.lastIndexOf("/")+1,ConfParser.url.length());
							dbName = dbName + "_" + code2;
							ConfParser.url = ConfParser.url+"_"+code2;
							System.out.println(ConfParser.url);
						}else{
							System.out.println(ConfParser.url);
							dbName = "testdataanalyse";
							ConfParser.url = ConfParser.url + "testdataanalyse";
							System.out.println(ConfParser.url);
						}
						System.out.println(ConfParser.url);
						try {
							insertMysqlDao.start();	
						}catch (MySQLSyntaxErrorException e) {
							insertMysqlDao.createDB(dbName);
							try {
								insertMysqlDao.start();
							} catch (Exception e1) {
								e1.printStackTrace();
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
						try {
							insertMysqlDao.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						String sr = GetPostTest.sendGet(codeUrl, codeStr);
						System.out.println(sr);
						JSONObject json = JSONObject.fromObject(sr);
						try {
							JSONObject thisOrgInfo = json.getJSONObject("thisOrgInfo");
							JSONArray jsonArray = json.getJSONArray("detail").isArray()?json.getJSONArray("detail"):new JSONArray();
							String keySelf = thisOrgInfo.getString("key");
							String storePathSelf = thisOrgInfo.getString("storepath");
							
							if (keySelf == null) {
								keySelf = "";
							}
							if (!keySelf.isEmpty()) {
								keyList.add(keySelf);
							}
							if (storePathSelf != null && !storePathSelf.isEmpty()) {
								storePathSelf = storePathSelf.replace("/", File.separator)
										.replace("\\", File.separator);
								if (storePathSelf.endsWith("Default")) {
									storePathSelf = storePathSelf.replace(File.separator
											+ "Default", "");
								}
								keyList.add(storePathSelf);
							}
							for (int i = 0; i < jsonArray.size(); i++) {
								JSONObject jsonObject = jsonArray.getJSONObject(i);
								String key	= jsonObject.getString("key")==null?"":jsonObject.getString("key");
								String storePath = jsonObject.getString("storepath");
								if (!key.isEmpty()) {
									keyList.add(key);
								}
								if (storePath != null && !storePath.isEmpty()) {
									storePath = storePath.replace("/", File.separator)
											.replace("\\", File.separator);
									if (storePath.endsWith("Default")) {
										storePath = storePath.replace(File.separator
												+ "Default", "");
									}
									keyList.add(storePath);
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							continue;
						}
						String paths[] = getPostTest.getPaths(desc.appid, keyList);
						for (int i = 0; i <paths.length; i++) {
							String filePath = paths[i];
							if (filePath == null || filePath.isEmpty()) {
								continue;
							}
							System.out.println(filePath);
							String org = "";
							while(filePath.endsWith(File.separator)){
								filePath = filePath.substring(0, filePath.length()-1);
							}
							org = filePath.substring(filePath.lastIndexOf(File.separator)+1, filePath.length());
							desc.findFile(filePath, org);
						}
					}
				}
			}
			
		}
//		if(desc.sqlList.size() > 0){
//			desc.dealData();			
//			desc.sqlList.clear();
//		}
		System.out.println("counts: " + desc.counts);
		System.out.println("successCount: " + successCount);
		System.out.println("failCount: " + failCount);
		desc.getAnalyzeThreadPool().shutdown();
		dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> End!");
		map.put("存储路径", mapCount++);
//		queryCassandraReport();
//		queryCassandraReportTest();
	}
	
	public List getAppids(){
		List appids = new ArrayList();
		String[] appnames =ConfParser.appname;
		boolean flag = false;
		String names = "";
		if(appnames!=null && appnames.length>0){
			names = "(";
			for (int i = 0; i <appnames.length; i++) {
				String appname = appnames[i];
				if(appname.equals("OTS APP 1.5系列") || appname.equals("OTS 1.5 for Android")){
					flag = true;
					continue;
				}else{
					if(!appname.isEmpty()){
						names += "'"+appname+"',";
					}
				}
			}
			while(names.endsWith(",")){
				names = names.substring(0, names.length()-1);
			}
			names += ")";
		}
		String sql = "";
		if(names.isEmpty() || names.equals("()")){
			if(flag){
				appids.add("default_app_id");
			}
			return appids;
		}else{
			sql = "SELECT app_id FROM static_param.b_appid_info where app_name_ch in "+names+" or app_name_en in "+names+";";
			appids = insertMysqlDao.getappIds(sql);
			if(flag){
				appids.add("default_app_id");
			}
			return appids;
		}
	}
	
	public void findFile(String rootDirectory, String org) {
		System.out.println("开始findFile************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		File rootFile = new File(rootDirectory);
		if(!rootFile.exists()){
//			rootFile.mkdir();
			return ;
		}
		File[] fileList = rootFile.listFiles(new FilenameFilter(){
            public boolean accept(File dir, String name){
            	if(dir.isDirectory() || name.indexOf(".summary.csv") >= 0 || name.indexOf(".abstract.csv") >= 0){
            		return true;
            	}
            	return false;
            }
        });
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		// long nums = list.length;
		StringBuffer errStr = new StringBuffer();
		try {
			for (int i = 0; i < fileList.length; i++) {
				File myFile = fileList[i];
				if (myFile.isDirectory()) {
					length = length + myFile.listFiles().length;
					findFile(myFile.getAbsolutePath(),org);
				}
				try {
					File filePath = new File(myFile.getAbsolutePath());
					try {
						this.deal(myFile, 01001,org);
					} catch (Exception e) {
						e.printStackTrace();
						errStr.append(myFile.getAbsolutePath() + ":\r\n");
						errStr.append(e.toString() + "\r\n");
						errors++;
						System.out.println("文件处理异常>>>"
								+ Paths.get(myFile.getAbsolutePath()));
						System.out.println("异常报告数为>>>" + errors);
					}
					// }

					// try {
					// Thread.sleep(1000000);
					// } catch (InterruptedException e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// }
					counts++;
					if ((counts % 100) == 0) {
						System.out.println("已处理报告数为：" + length);
						// System.out.println("已处理报告数为："+counts+"/"+nums);
					}
				} catch (Exception e) {
					e.printStackTrace();
					errStr.append(myFile.getAbsolutePath() + ":\r\n");
					errStr.append(e.getMessage() + "\r\n");
				}
			}
			System.out.println(dateString + ">>> 发现待处理文件数量为： " + length);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	// 85.228:9160
	private ArrayList<String> getFolderList(String rootDirectory,
			ArrayList<String> folderList) {
		File file = new File(rootDirectory);
		File[] list = file.listFiles();
		for (int i = 0; i < list.length; i++) {
			File sonFile = list[i];
			if (sonFile.isDirectory()) {
				folderList.add(sonFile.getAbsolutePath());
//				folderList = getFolderList(sonFile.getAbsolutePath(),
//						folderList);
			}
		}
		return folderList;
	}

	public void deal(File file, int ServiceId,String org) {
		if (file.getAbsoluteFile().toString().endsWith(".abstract.csv")
				|| file.getAbsoluteFile().toString().endsWith(".summary.csv")) {
			System.out.println("开始deal************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
			splitAbstractCsv(file, 01001,org);
			setCount();
		} else {
			return;
		}
	}

	public boolean splitAbstractCsv(File filePath, int ServiceId,String org) {
		System.out.println("开始splitAbstractCsv************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		StringBuffer errStr1 = new StringBuffer("");
		try {
			String datetime_body = "";
			String datetime_header = "";
			String datetime = "";
			ArrayList<String[]> absHeader = new ArrayList<String[]>();
			ArrayList<String[]> absBody = new ArrayList<String[]>();
			ArrayList<String[]> testScenario = new ArrayList<String[]>();
			ArrayList<String[]> csvList = this.readCsv(filePath);
			int dataEreaIndex = 0;
			if (ServiceId == 03001) {
				for (int i = 0; i < csvList.size(); i++) {
					if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
						if (csvList.get(i)[0] != null) {
							if (csvList.get(i)[0].equals("测试时间")
									|| csvList.get(i)[0].equals("测速类型")) {
								dataEreaIndex = 1;
							} else if (csvList.get(i)[0]
									.equals("--- Test Scenario ---")) {
								dataEreaIndex = 2;
							}
						}
						if (dataEreaIndex == 0) {
							absHeader.add(csvList.get(i));
						} else if (dataEreaIndex == 1) {
							absBody.add(csvList.get(i));
						} else if (dataEreaIndex == 2) {
							testScenario.add(csvList.get(i));
						}
					} else {
						dataEreaIndex++;
					}
				}
			} else if (ServiceId == 01001) {
				if(csvList==null || csvList.size()==0){
					return false;
				}
				for (int i = 0; i < csvList.size(); i++) {
					if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
						if (csvList.get(i)[0] != null) {
							if ((csvList.get(i)[0].equals("测试时间")||csvList.get(i)[0].trim().toLowerCase().equals("time") || csvList.get(i)[0].trim().toLowerCase().equals("test time")) 
									&& csvList.size() > i + 1
									&& csvList.get(i + 1) != null
									&& csvList.get(i + 1).length >= 2
									&& csvList.get(i + 1)[0] != null
									&& (csvList.get(i + 1)[0].contains("型号") ||(csvList.get(i + 1)[0].trim().toLowerCase().contains("device type") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal type") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal mode") || csvList.get(i + 1)[0].trim().toLowerCase().contains("device model") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal model")))) {
							
							} else if ((csvList.get(i)[0].equals("测试时间")
									|| csvList.get(i)[0].equals("测试开始时间")
									|| csvList.get(i)[0].equals("时间")
									|| (csvList.get(i)[0].equals("测速类型") || csvList.get(i)[0].equals("测试类型"))
									|| csvList.get(i)[0].equals("网络制式1时间")
									|| csvList.get(i)[0].equals("网络(1)时间")
									|| csvList.get(i)[0].equals("Network 1Time")
									|| csvList.get(i)[0].equals("Network(1) Time")
									|| csvList.get(i)[0].trim().toLowerCase().equals("time")
									|| csvList.get(i)[0].trim().toLowerCase().equals("duration") 
									|| csvList.get(i)[0].trim().toLowerCase().equals("start time")
									|| csvList.get(i)[0].trim().toLowerCase().equals("test start time")
									|| (csvList.get(i)[0].trim().toLowerCase().equals("time") &&(csvList.get(i)[1]!=null && csvList.get(i)[1].trim().toLowerCase().equals("network") || csvList.get(i)[1].trim().toLowerCase().equals("website") || csvList.get(i)[1].trim().toLowerCase().equals("address"))))
									&& csvList.get(i).length > 2 && csvList.get(i)[2]!=null && !csvList.get(i)[2].equals("")
									){
								dataEreaIndex = 1;
							} else if (csvList.get(i)[0].contains("Test Scenario")) {
								dataEreaIndex = 2;
							}
						}
						if (dataEreaIndex == 0) {
							absHeader.add(csvList.get(i));
						} else if (dataEreaIndex == 1) {
							absBody.add(csvList.get(i));
						} else if (dataEreaIndex == 2) {
							testScenario.add(csvList.get(i));
						}
					} else {
					}
				}
			}
			this.writeToFile(absHeader, absBody, testScenario, filePath,org);
		} catch (Exception e) {
			copyErrFile(filePath);
			errStr1.append(filePath.getAbsolutePath() + ":\r\n");
			errStr1.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		return true;
	}

	private void writeToFile(ArrayList<String[]> absHeader,
			ArrayList<String[]> absBody, ArrayList<String[]> testScenario,
			File filePath, String org) {
		System.out.println("开始writeToFile************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		StringBuffer errStr2 = new StringBuffer("");
		// 获取测试类型Num
		Pattern pattern1 = Pattern.compile("[-_.]");
		Pattern pattern2 = Pattern.compile("\\d+");
		String numStrs[] = pattern1.split(filePath.getName());
		String datatime[] = null;
		if(absBody==null || absBody.size()==0){
			datatime = new String[1];
		}else{
			datatime = new String[absBody.size()];
		}
		// 要生成的TXT的文件内容
		StringBuffer str = new StringBuffer();
		// 头部Map
		Map absHeaderMap = new HashMap();
		// 主体Map
		Map absBodyMap = new HashMap();
		// testScenario Map
		Map testScenarioMap = new HashMap();
		try {
			for (int i = 0; i < absHeader.size(); i++) {
				String key = absHeader.get(i)[0];
				try {
					String value = "";
					if(absHeader.get(i).length>=2){
						value = absHeader.get(i)[1];
					}
					if ((key.contains("测试时间") || key.equals("测试开始时间") || key.equals("时间")
						||key.trim().toLowerCase().contains("time") 
						|| key.trim().toLowerCase().equals("start time")
						|| key.trim().toLowerCase().equals("test start time"))
						) {
							datatime[0] = value;
						}
					absHeaderMap.put(key, value);
				} catch (ArrayIndexOutOfBoundsException e) {
					absHeaderMap.put(key, "");
					copyErrFile(filePath);
					errStr2.append(filePath.getAbsolutePath() + ":\r\n");
					errStr2.append(e.toString() + "\r\n");
					e.printStackTrace();
				} catch (Exception e) {
					absHeaderMap.put(key, "");
					copyErrFile(filePath);
					errStr2.append(filePath.getAbsolutePath() + ":\r\n");
					errStr2.append(e.toString() + "\r\n");
					e.printStackTrace();
				}
			}
			absHeaderMap.put("测试类型Num", numStrs[1]);
			for (int i = 0; i < testScenario.size(); i++) {
				String key = testScenario.get(i)[0];
				try {
					if (key != null) {
						String value = "";
						if (i != 0) {
							if(testScenario.get(i).length>=2){
								value = testScenario.get(i)[1];	
							}
						}
						if (key.equals("")
								&& testScenario.get(i + 1).length >= 2
								&& testScenario.get(i + 1)[0] != null) {
							testScenarioMap.put(key, value);
						} else if (!key.equals("")) {
							testScenarioMap.put(key, value);
						}
					}

				} catch (ArrayIndexOutOfBoundsException e) {
					testScenarioMap.put(key, "");
					copyErrFile(filePath);
					errStr2.append(filePath.getAbsolutePath() + ":\r\n");
					errStr2.append(e.toString() + "\r\n");
					e.printStackTrace();
				} catch (Exception e) {
					testScenarioMap.put(key, "");
					copyErrFile(filePath);
					errStr2.append(filePath.getAbsolutePath() + ":\r\n");
					errStr2.append(e.toString() + "\r\n");
					e.printStackTrace();
				}
			}
			if (absBody != null && absBody.size() > 0) {
				String[] key = absBody.get(0);// 主体的表头
				if (key != null) {
					for (int i = 1; i < absBody.size(); i++) {// 主体的数据
						try{					// 第i排数据，i从1开始
							String[] value = absBody.get(i);
							if (!isBlankLine(value)) {
								Map map = new HashMap();
								for (int j = 0; j < key.length; j++) {
									String sonKey = key[j];
									String sonValue = "";
									try {
										sonValue = value[j];	
									} catch (Exception e) {
										sonValue = "";
									}
									map.put(sonKey, sonValue);
								}
								if ((key[0].contains("测试时间") || key[0].equals("测试开始时间") || key[0].equals("时间")
										||key[0].trim().toLowerCase().equals("time") || key[0].trim().toLowerCase().equals("start time") || key[0].trim().toLowerCase().equals("test start time") || key[0].trim().toLowerCase().equals("network 1time"))
										) {
											datatime[i-1] = value[0];
										}
								absBodyMap.put(i, map);
							}
						} catch (ArrayIndexOutOfBoundsException e) {
							absBodyMap.put(key, "");
							copyErrFile(filePath);
							errStr2.append(filePath.getAbsolutePath() + ":\r\n");
							errStr2.append(e.toString() + "\r\n");
							e.printStackTrace();
						} catch (Exception e) {
							absBodyMap.put(key, "");
							copyErrFile(filePath);
							errStr2.append(filePath.getAbsolutePath() + ":\r\n");
							errStr2.append(e.toString() + "\r\n");
							e.printStackTrace();
						}
					}
				}
			}
			if (absBodyMap != null) {
				if (absBodyMap.size() == 1) {
					Map nextMap = (Map) absBodyMap.get(1);
					String body = nextMap.toString();
					String header = absHeaderMap.toString();
					String testScenarip = testScenarioMap.toString();

					body = subTxt(body);
					header = subTxt(header);
					testScenarip = subTxt(testScenarip);

					str.append(body);
					if (!body.trim().equals("") && !header.trim().equals("")) {
						str.append(",");
					}
					str.append(header);
					if (!header.trim().equals("")
							&& !testScenarip.trim().equals("")) {
						str.append(",");
					}
					str.append(testScenarip);
				} else if (absBodyMap.size() > 1) {
					for (int i = 0; i < absBodyMap.size(); i++) {
						Map nextMap = (Map) absBodyMap.get(i + 1);
						String body = nextMap.toString();
						String header = absHeaderMap.toString();
						String testScenarip = testScenarioMap.toString();

						body = subTxt(body);
						header = subTxt(header);
						testScenarip = subTxt(testScenarip);

						str.append(body);
						if (!body.trim().equals("")
								&& !header.trim().equals("")) {
							str.append(",");
						}
						str.append(header);
						if (!header.trim().equals("")
								&& !testScenarip.trim().equals("")) {
							str.append(",");
						}
						str.append(testScenarip);
						str.append("\r\n");
					}
				} else {
					String header = absHeaderMap.toString();
					String testScenarip = testScenarioMap.toString();

					header = subTxt(header);
					testScenarip = subTxt(testScenarip);

					str.append(header);
					if (!header.trim().equals("")
							&& !testScenarip.trim().equals("")) {
						str.append(",");
					}
					str.append(testScenarip);
				}
			}

		} catch (Exception e) {
			copyErrFile(filePath);
			errStr2.append(filePath.getAbsolutePath() + ":\r\n");
			errStr2.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		System.out.println("开始writeToFile中格式化时间************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		try {
			//格式化时间
			String dataLong[] = new String[datatime.length];
			for (int i = 0; i < datatime.length; i++) {
				String datatimeStr = datatime[i];
				Date date1 = null;
				try {
					if(datatimeStr==null || datatimeStr.isEmpty()){
						dataLong[i] = "";	
					}else{
						String timeStrs[] = pattern2.split(datatimeStr);
						StringBuffer rex = new StringBuffer("");
						for (int j = 0; j < timeStrs.length; j++) {
							rex.append(timeStrs[j]);
							rex.append(dateStr[j]);
						}
						SimpleDateFormat formatter = new SimpleDateFormat(
								rex.toString());
						date1 = formatter.parse(datatimeStr);	
						dataLong[i] = Long.toString(date1.getTime());	
					}
					
				} catch (ParseException e) {
					e.printStackTrace();
					System.out.println("转换时间有误");
				}
			}
			String prefix = ConfParser.org_prefix;
//			String org = filePath.getAbsolutePath().replace(prefix, "");
//			//去除后缀之后的下一层目录名
//			String orgStr = org.substring(0,org.indexOf(File.separator));
//			if(appid.contains(orgStr)){
//				//新版带appid目录的获取方式
//				org = org.substring(org.indexOf(File.separator),org.length());
//				while(org.startsWith(File.separator)){
//					org = org.substring(org.indexOf(File.separator)+1,org.length());
//				}
//				org = org.substring(0,org.indexOf(File.separator));
//				
//			}else{
//				//原版不带appid目录的获取方式
//				org = org.substring(0,org.indexOf(File.separator));
//			}
//			org = org.substring(0,org.indexOf(File.separator));
			System.out.println("keyspace:" + org);
			String imei = filePath.getParentFile().getName();
			setMapData(absHeaderMap, absBodyMap, testScenarioMap, numStrs[1],
					filePath.getAbsoluteFile(), dataLong, org, imei);

		} catch (Exception e) {
			copyErrFile(filePath);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void createKeyspace(String numType, String keyspace) {/*
		KeyspaceDefinition kd = cluster.describeKeyspace(keyspace);
		if (numType.equals("03001")) {
			int i = 0;
			i++;
		}
		if (kd != null) {
			List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
			for (int i = 0; i < cfList.size(); i++) {
				ColumnFamilyDefinition cfD = cfList.get(i);
				if (cfD.getName().equals(numType)) {
					return;
				}
			}
			
			BasicColumnDefinition dateColumnDefinition = new BasicColumnDefinition();
			dateColumnDefinition.setName(stringSerializer
					.toByteBuffer("year_month_day"));
			dateColumnDefinition.setIndexName(numType + "_"
					+ "year_month_day_idx");
			dateColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			dateColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			BasicColumnDefinition imeiColumnDefinition = new BasicColumnDefinition();
			imeiColumnDefinition.setName(stringSerializer
					.toByteBuffer("imei"));
			imeiColumnDefinition.setIndexName(numType + "_"
					+ "imei_idx");
			imeiColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			imeiColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			BasicColumnDefinition fileIndexColumnDefinition = new BasicColumnDefinition();
			fileIndexColumnDefinition.setName(stringSerializer
					.toByteBuffer("file_index"));
			fileIndexColumnDefinition.setIndexName(numType + "_"
					+ "file_index_idx");
			fileIndexColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			fileIndexColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			// 字段操作
			BasicColumnDefinition dataTimeColumnDefinition = new BasicColumnDefinition();
			dataTimeColumnDefinition.setName(stringSerializer
					.toByteBuffer("data_time"));
			dataTimeColumnDefinition.setIndexName(numType + "_"
					+ "data_time_idx");
			dataTimeColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			dataTimeColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());

			ColumnFamilyDefinition cfDef = HFactory
					.createColumnFamilyDefinition(keyspace, numType,
							ComparatorType.UTF8TYPE);
			// 下面两行我加的，为了让key和value以UTF8格式保存
			cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
			cfDef.setDefaultValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			cfDef.addColumnDefinition(dateColumnDefinition);
			cfDef.addColumnDefinition(dataTimeColumnDefinition);
			cfDef.addColumnDefinition(imeiColumnDefinition);
			cfDef.addColumnDefinition(fileIndexColumnDefinition);
			
			try {
				cluster.addColumnFamily(cfDef);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			return;
		} else {

			BasicColumnDefinition dateColumnDefinition = new BasicColumnDefinition();
			dateColumnDefinition.setName(stringSerializer
					.toByteBuffer("year_month_day"));
			dateColumnDefinition.setIndexName(numType + "_"
					+ "year_month_day_idx");
			dateColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			dateColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());

			BasicColumnDefinition imeiColumnDefinition = new BasicColumnDefinition();
			imeiColumnDefinition.setName(stringSerializer
					.toByteBuffer("imei"));
			imeiColumnDefinition.setIndexName(numType + "_"
					+ "imei_idx");
			imeiColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			imeiColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			BasicColumnDefinition fileIndexColumnDefinition = new BasicColumnDefinition();
			fileIndexColumnDefinition.setName(stringSerializer
					.toByteBuffer("file_index"));
			fileIndexColumnDefinition.setIndexName(numType + "_"
					+ "file_index_idx");
			fileIndexColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			fileIndexColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());
			
			
			// 字段操作
			BasicColumnDefinition dataTimeColumnDefinition = new BasicColumnDefinition();
			dataTimeColumnDefinition.setName(stringSerializer
					.toByteBuffer("data_time"));
			dataTimeColumnDefinition.setIndexName(numType + "_"
					+ "data_time_idx");
			dataTimeColumnDefinition.setIndexType(ColumnIndexType.KEYS);
			dataTimeColumnDefinition.setValidationClass(ComparatorType.UTF8TYPE
					.getClassName());

			BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
			columnFamilyDefinition.setKeyspaceName(keyspace);
			columnFamilyDefinition.setName(numType);
			columnFamilyDefinition
					.addColumnDefinition(dateColumnDefinition);
			columnFamilyDefinition
					.addColumnDefinition(imeiColumnDefinition);
			columnFamilyDefinition
					.addColumnDefinition(fileIndexColumnDefinition);
			columnFamilyDefinition
					.addColumnDefinition(dataTimeColumnDefinition);
			ColumnFamilyDefinition cfReport = new ThriftCfDef(
					columnFamilyDefinition);
			KeyspaceDefinition keyspaceDefinition = HFactory
					.createKeyspaceDefinition(keyspace,
							"org.apache.cassandra.locator.SimpleStrategy", 1,
							Arrays.asList(cfReport));
			cluster.addKeyspace(keyspaceDefinition);
		}
	*/}

	public static void queryCassandra(Map dataMap, String numType, File file,
			String keyspace) {/*
		String columnFamily = numType;
		ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
		Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();

		clmap.put(columnFamily, HConsistencyLevel.ONE);
		configurableConsistencyLevel.setReadCfConsistencyLevels(clmap);
		configurableConsistencyLevel.setWriteCfConsistencyLevels(clmap);

		Keyspace keySpace = HFactory.createKeyspace(keyspace, cluster,
				configurableConsistencyLevel);
		RangeSlicesQuery<String, String, Long> rangeQuery = HFactory
				.createRangeSlicesQuery(keySpace, stringSerializer,
						stringSerializer, longSerializer);

		rangeQuery.setColumnFamily(columnFamily);
		rangeQuery.setColumnNames("key", "value");

		QueryResult<OrderedRows<String, String, Long>> result = rangeQuery
				.execute();
		OrderedRows<String, String, Long> oRows = result.get();

		List<Row<String, String, Long>> rowList = oRows.getList();
		for (Row<String, String, Long> row : rowList) {
			List<HColumn<String, Long>> columns = row.getColumnSlice()
					.getColumns();
			for (HColumn<String, Long> hColumn : columns) {
				if ("value".equals(hColumn.getName())) {
					System.out.println("  column name: " + hColumn.getName()
							+ ": " + hColumn.getValue());
				} else {
					System.out.println("  column name: "
							+ hColumn.getName()
							+ ": "
							+ stringSerializer.fromByteBuffer(hColumn
									.getValueBytes()));
				}
			}

		}
	*/}
	
	public static String queryExist(Keyspace keyspace,String cf,String columnFamily,String key){
		String value = "0";
		SliceQuery<String, String, String> query = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
		query.setColumnFamily(cf);
		query.setKey(key);
		//ColumnSliceIterator指column的名称值
		ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);
		//ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, "C", "g", false);
		while (iterator.hasNext()) {
			HColumn<String, String> element = iterator.next();
			if(element.getName().equals(columnFamily)){
				value = element.getValue();
			}
			System.out.println(element.getName() + ":" + element.getValue());
		}
		if(value==null || value.trim().equals("")){
			value = "0";
		}
		return value;
	}
	
	public static void queryCassandraReport() {/*
		String cFtotalReport = "total_report";
		String cFtotalGps = "total_gps";
		List<KeyspaceDefinition> ks = cluster.describeKeyspaces();
		createKeyspace("geo_code", "GEO_DATA");
//		for (int k = 0; k < ks.size(); k++) {
//			KeyspaceDefinition ksd = ks.get(k);
		String kss[] = new String[]{"VOICE_MOS_CLOUD"};//{"CMCC_CMRI_DEPT_BD_PINGTAI","CMCC_CMRI_DEPT_BD_SHANGWU","CMCC_CMRI_DEPT_BD_SHICHANG","CMCC_CMRI_DEPT_DASHUJUYINGYONG","CMCC_CMRI_DEPT_HULIANWANGPINGTAI_HLWNLPT","CMCC_CMRI_DEPT_HULIANWANGPINGTAI_YHKYSFGL","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_CPGHSJYYY","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_CPYFE","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_CPYFY","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_ZLBZ","CMCC_CMRI_DEPT_ITYUNJISUAN","CMCC_CMRI_DEPT_YEWU_DUOMEITICHANPIN","CMCC_CMRI_DEPT_YUNJISUAN_ITJISHU","CMCC_CMRI_DEPT_YUNJISUAN_YEWUZHICHENG","CMCC_CMRI_DEPT_ZHICHENGXITONG","CMCC_CMRI_DEPT_ZONGHE","CMCC_CMRI_DEPT_WUXIAN_WXCPRJGN","CMCC_CMRI_DEPT_KEGUAN_ZHUANLIFAWU","CMCC_CMRI_DEPT_WULIAN_ZHONGDUAN","CMCC_CMRI_DEPT_WUXIAN_WXCPYJJS","CMCC_CMRI_DEPT_XINSHICHANGYANJIUZHONGXIN"};
		for (int k = 0; k < kss.length; k++) {
			String kssName = kss[k];
			KeyspaceDefinition ksd = cluster.describeKeyspace(kssName);
			if(ksd==null){
				continue;
			}
			if("system".equals(ksd.getName()) || "system_traces".equals(ksd.getName()) || "OpsCenter".equals(ksd.getName())){
				continue;
			}
			String keySpaceStr = ksd.getName();
			KeyspaceDefinition kd = cluster.describeKeyspace(ksd.getName());
			List<ColumnFamilyDefinition> cfList = kd.getCfDefs();
			for (int i = 0; i < cfList.size(); i++) {
				String columnFamily = cfList.get(i).getName();
				if(cluster.describeKeyspace(keySpaceStr)==null){
					createKeyspace(columnFamily, keySpaceStr);
				}
				Keyspace keyspace = HFactory.createKeyspace(keySpaceStr,cluster);
				
				RangeSlicesQuery<String, String, String> rangeQuery = HFactory
						.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
				
				rangeQuery.setColumnFamily(columnFamily);
				rangeQuery.setKeys("", "");
				rangeQuery.setRowCount(60000);
				rangeQuery.setColumnNames("GPS位置","测试位置","测试GPS位置","GPS位置信息","GPS信息","cassandra_province","insert_state");
				
				QueryResult<OrderedRows<String, String, String>> result = null;
				try{
					result = rangeQuery.execute();
				}catch (Exception e){
					try{
						result = rangeQuery.execute();
					}catch (Exception e1){
						try{
							result = rangeQuery.execute();
						}catch (Exception e2){
							e.printStackTrace();
						}
						e.printStackTrace();
						continue;
					}
					e.printStackTrace();
				}
				OrderedRows<String, String, String> oRows = result.get();
				List<Row<String, String, String>> rowList = oRows.getList();
				for (Row<String, String, String> row : rowList) {
		//			System.out.println("key: " + row.getKey());
					String gpsStr =  "";
					String columnName = "";
					boolean hasInsert = false;
					List<HColumn<String, String>> columns = row.getColumnSlice()
							.getColumns();
					Map<String, String> gpsMap = new HashMap<String, String>();
					String insertState = "0";
					for (HColumn<String, String> hColumn : columns) {
						if("insert_state".equals(hColumn.getName())){
							insertState = hColumn.getValue();
							continue;
						}
						gpsMap.put(hColumn.getName(), hColumn.getValue());
					}
					if(gpsMap.containsKey("cassandra_province")){
						String province = gpsMap.get("GPS信息");
						if(province!=null && !province.equals("")){
							hasInsert = true;
						}
					}
					if(gpsMap.containsKey("GPS信息")){
						columnName = "GPS信息";
						gpsStr = gpsMap.get("GPS信息");
					}else if(gpsMap.containsKey("GPS位置信息")){
						columnName = "GPS位置信息";
						gpsStr = gpsMap.get("GPS位置信息");
					}else if(gpsMap.containsKey("GPS位置")){
						columnName = "GPS位置";
						gpsStr = gpsMap.get("GPS位置");
					}else if(gpsMap.containsKey("测试位置")){
						columnName = "测试位置";
						gpsStr = gpsMap.get("测试位置");
					}else if(gpsMap.containsKey("测试GPS位置")){
						columnName = "测试GPS位置";
						gpsStr = gpsMap.get("测试GPS位置");
					}else{
						columnName = "";
						gpsStr = "";
					}
					if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("") && !insertState.equals("14") && !hasInsert){
						try {
							//插入gps信息
							Keyspace keyspaceOperator = HFactory.createKeyspace(keySpaceStr,
									cluster);
							Mutator<String> userMutator = HFactory.createMutator(
									keyspaceOperator, stringSerializer);
							if(gpsStr.contains(" ")){
								
								String latitude = "";
								String longitude = "";
								
								String[] gps = transGpsPoint(gpsStr);
								
								if(gps!=null && gps[0]!=null && gps[1]!=null){
									longitude = gps[0];
									latitude = gps[1];
								}
								
								DecimalFormat decimalFormat=new DecimalFormat(".000000");
								String key1 = decimalFormat.format(Float.valueOf(latitude));
								String key2 = decimalFormat.format(Float.valueOf(longitude));
								String spaces_key = key1+"_"+key2;
								
								//查询GET_DATA数据库中是否有此地址信息 否查询百度API
								String spaceGpsCass = "";
								Keyspace GPS_keyspace = HFactory.createKeyspace("GEO_DATA",cluster);
								SliceQuery<String, String, String> query = HFactory.createSliceQuery(GPS_keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
								query.setColumnFamily("geo_code");
								query.setKey(spaces_key);
								query.setColumnNames("city","district","province","street","street_number");
								
								//ColumnSliceIterator指column的名称值
								ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);
								boolean hasValue = false;
								
								String province = "";
								String city = "";
								String district = "";
								String street = "";
								String street_number = "";
								while (iterator.hasNext()) {
									HColumn<String, String> element = iterator.next();
									if(element.getName().equals("province")){
										province = element.getValue();
									}else if(element.getName().equals("city")){
										city = element.getValue();
									}else if(element.getName().equals("district")){
										district = element.getValue();
									}else if(element.getName().equals("street")){
										street = element.getValue();
									}else if(element.getName().equals("street_number")){
										street_number = element.getValue();
									}
									System.out.println(element.getName() + ":" + element.getValue());
									System.out.println(spaces_key+" 初级命中缓存");
								}
								if(province.equals("")||province.equals("-")||province.equals("N/A")){
									hasValue = false;
								}else{
									hasValue = true;
								}
								
								spaceGpsCass = province+"_"+city+"_"+district+"_"+street+"_"+street_number;
								String spaceGps = "";
								String spaceGpsStr = "";
								if(hasValue){
									spaceGpsStr = spaceGpsCass;
									System.out.println(spaces_key+" 真实命中缓存" + spaceGpsStr);
								}else{
									String resp = testPost(latitude, longitude);
									spaceGpsStr = subLalo(resp);
									
									//插入GEO_DATA信息
									if(cluster.describeKeyspace("GEO_DATA")==null){
										createKeyspace("geo_code", "GEO_DATA");
									}
									Keyspace keyspace_geo = HFactory.createKeyspace("GEO_DATA", cluster);
									Mutator<String> spaceMutator = HFactory.createMutator(
											keyspace_geo, stringSerializer);
									String spaces[] = spaceGpsStr.split("_");
									String spaceName [] = new String[]{"province","city","district","street","street_number"};
									if(spaces.length==5){
										for (int j = 0; j < spaces.length; j++) {
											spaceMutator.addInsertion(spaces_key, "geo_code",
													HFactory.createStringColumn(spaceName[j],spaces[j]));
										}
										spaceMutator.execute();
									}
								}
								
								createKeyspace(cFtotalGps,keySpaceStr);
								createKeyspace(cFtotalReport,keySpaceStr);
								
								String spaces[] = spaceGpsStr.split("_");
								if(spaces.length==5){
									String spaceName1 [] = new String[]{"cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number"};
									spaceGps = spaces[0]+"_"+spaces[1]+"_"+spaces[2];
									for (int j = 0; j < spaces.length; j++) {
										
										//同时修改原数据
										userMutator.addInsertion(row.getKey(), columnFamily,
												HFactory.createStringColumn(spaceName1[j],spaces[j]));
									}
									userMutator.addInsertion(row.getKey(), columnFamily,
											HFactory.createStringColumn("insert_state","14"));
									userMutator.execute();
								}
								
								//插入gps信息
								String keyStr = row.getKey();
								String yearMonthDay = getYearMonthDay(keyspace, columnFamily,keyStr);
								if(yearMonthDay!=null){
									spaceGps = spaceGps+"_"+yearMonthDay;
								}
								String value = queryExist(keyspace,cFtotalGps,columnFamily,spaceGps);
								System.out.println("key:"+spaceGps+",cf:"+cFtotalGps+",value_key"+columnFamily+",value:"+value);
								
								//插入省市县的信息
								String spaceName1 [] = new String[]{"province","city","district","street","street_number"};
								for (int j = 0; j < 3; j++) {
									userMutator.addInsertion(spaceGps, cFtotalGps,
											HFactory.createStringColumn(spaceName1[j],spaces[j]));
									userMutator.addInsertion(spaceGps, cFtotalReport,
											HFactory.createStringColumn(spaceName1[j],spaces[j]));
								}
								userMutator.addInsertion(spaceGps, cFtotalGps,
										HFactory.createStringColumn("year_month_day", yearMonthDay));//插入年月日的数据
								userMutator.addInsertion(spaceGps, cFtotalGps,
										HFactory.createStringColumn(columnFamily, (Integer.parseInt(value)+1)+""));	//插入统计信息
								
								userMutator.execute();
								
								//插入report信息
								if(row.getKey().endsWith("monitor.csv_0_0") || row.getKey().endsWith("abstract.csv_0") || row.getKey().endsWith("summary.csv_0")){
									String value1 = queryExist(keyspace,cFtotalReport,columnFamily,spaceGps);
									System.out.println("key:"+spaceGps+",cf:"+cFtotalReport+",value_key"+columnFamily+",value:"+value1);
									userMutator.addInsertion(spaceGps, cFtotalReport,
											HFactory.createStringColumn("year_month_day", yearMonthDay));//插入年月日的数据
									userMutator.addInsertion(spaceGps, cFtotalReport,
											HFactory.createStringColumn(columnFamily, (Integer.parseInt(value1)+1)+""));//插入统计信息
									userMutator.execute();
								}
							}
						}catch (HTimedOutException e) {
							e.printStackTrace();
						} catch (HectorException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		System.out.println("更新GPS数据完成");
	*/}
	
	public static void queryCassandraReportTest() {/*
		String cFtotalReport = "total_report";
		String cFtotalGps = "total_gps";
		KeyspaceDefinition myKsd = cluster.describeKeyspace("CMCC_CMRI_DEPT_CESHI_YEWUSHI");
//		List<KeyspaceDefinition> ks = cluster.describeKeyspaces();
		List<KeyspaceDefinition> ks = new ArrayList<KeyspaceDefinition>();
		ks.add(myKsd);
		createKeyspace("geo_code", "GEO_DATA");
		for (int k = 0; k < ks.size(); k++) {
			KeyspaceDefinition ksd = ks.get(k);
//		String kss[] = new String[]{"CMCC_CMRI_DEPT_ANQUAN_WANGLUOANQUAN"};//{"CMCC_CMRI_DEPT_BD_PINGTAI","CMCC_CMRI_DEPT_BD_SHANGWU","CMCC_CMRI_DEPT_BD_SHICHANG","CMCC_CMRI_DEPT_DASHUJUYINGYONG","CMCC_CMRI_DEPT_HULIANWANGPINGTAI_HLWNLPT","CMCC_CMRI_DEPT_HULIANWANGPINGTAI_YHKYSFGL","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_CPGHSJYYY","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_CPYFE","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_CPYFY","CMCC_CMRI_DEPT_HULIANWANGYINGYONG_ZLBZ","CMCC_CMRI_DEPT_ITYUNJISUAN","CMCC_CMRI_DEPT_YEWU_DUOMEITICHANPIN","CMCC_CMRI_DEPT_YUNJISUAN_ITJISHU","CMCC_CMRI_DEPT_YUNJISUAN_YEWUZHICHENG","CMCC_CMRI_DEPT_ZHICHENGXITONG","CMCC_CMRI_DEPT_ZONGHE","CMCC_CMRI_DEPT_WUXIAN_WXCPRJGN","CMCC_CMRI_DEPT_KEGUAN_ZHUANLIFAWU","CMCC_CMRI_DEPT_WULIAN_ZHONGDUAN","CMCC_CMRI_DEPT_WUXIAN_WXCPYJJS","CMCC_CMRI_DEPT_XINSHICHANGYANJIUZHONGXIN"};
//		for (int k = 0; k < kss.length; k++) {
//			String kssName = kss[k];
//			KeyspaceDefinition ksd = cluster.describeKeyspace(kssName);
//			if(ksd==null){
//				continue;
//			}
			if("system".equals(ksd.getName()) || "system_traces".equals(ksd.getName()) || "OpsCenter".equals(ksd.getName())){
				continue;
			}
			String keySpaceStr = ksd.getName();
			KeyspaceDefinition kd = cluster.describeKeyspace(ksd.getName());
			List<ColumnFamilyDefinition> cfListTest = kd.getCfDefs();
			List<ColumnFamilyDefinition> cfList = new ArrayList<ColumnFamilyDefinition>();
			for(ColumnFamilyDefinition myCFDT: cfListTest){
				if(myCFDT.getName().equals("04002")){
					cfList.add(myCFDT);
				}
			}
			for (int i = 0; i < cfList.size(); i++) {
				String columnFamily = cfList.get(i).getName();
				if(cluster.describeKeyspace(keySpaceStr)==null){
					createKeyspace(columnFamily, keySpaceStr);
				}
				Keyspace keyspace = HFactory.createKeyspace(keySpaceStr,cluster);
				
				RangeSlicesQuery<String, String, String> rangeQuery = HFactory
						.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
				
				rangeQuery.setColumnFamily(columnFamily);
				rangeQuery.setKeys("0b9c38aaf32d9ab60493b735c26a899c584633fb|00000000_04002.000_0-2014_10_16_07_25_28_924.abstract.csv_0", "0b9c38aaf32d9ab60493b735c26a899c584633fb|00000000_04002.000_0-2014_10_16_07_25_28_924.abstract.csv_0");
				rangeQuery.setRowCount(60000);
				rangeQuery.setColumnNames("GPS位置","测试位置","测试GPS位置","GPS位置信息","GPS信息","insert_state");
				
				QueryResult<OrderedRows<String, String, String>> result = rangeQuery
						.execute();
				OrderedRows<String, String, String> oRows = result.get();
				List<Row<String, String, String>> rowList = oRows.getList();
				for (Row<String, String, String> row : rowList) {
		//			System.out.println("key: " + row.getKey());
					String gpsStr =  "";
					String columnName = "";
					List<HColumn<String, String>> columns = row.getColumnSlice()
							.getColumns();
					Map<String, String> gpsMap = new HashMap<String, String>();
					String insertState = "0";
					for (HColumn<String, String> hColumn : columns) {
						if("insert_state".equals(hColumn.getName())){
							insertState = hColumn.getValue();
							continue;
						}
						gpsMap.put(hColumn.getName(), hColumn.getValue());
					}
					if(gpsMap.containsKey("GPS信息")){
						columnName = "GPS信息";
						gpsStr = gpsMap.get("GPS信息");
					}else if(gpsMap.containsKey("GPS位置信息")){
						columnName = "GPS位置信息";
						gpsStr = gpsMap.get("GPS位置信息");
					}else if(gpsMap.containsKey("GPS位置")){
						columnName = "GPS位置";
						gpsStr = gpsMap.get("GPS位置");
					}else if(gpsMap.containsKey("测试位置")){
						columnName = "测试位置";
						gpsStr = gpsMap.get("测试位置");
					}else if(gpsMap.containsKey("测试GPS位置")){
						columnName = "测试GPS位置";
						gpsStr = gpsMap.get("测试GPS位置");
					}else{
						columnName = "";
						gpsStr = "";
					}
					if(gpsStr != null && !gpsStr.trim().equals("--") && !gpsStr.trim().equals("-") && !gpsStr.trim().equals("") && !insertState.equals("14")){
						try {
							//插入gps信息
							Keyspace keyspaceOperator = HFactory.createKeyspace(keySpaceStr,
									cluster);
							Mutator<String> userMutator = HFactory.createMutator(
									keyspaceOperator, stringSerializer);
							if(gpsStr.contains(" ")){
								
								String latitude = "";
								String longitude = "";
								
								String[] gps = transGpsPoint(gpsStr);
								
								if(gps!=null && gps[0]!=null && gps[1]!=null){
									longitude = gps[0];
									latitude = gps[1];
								}
								
								DecimalFormat decimalFormat=new DecimalFormat(".000000");
								String key1 = decimalFormat.format(Float.valueOf(latitude));
								String key2 = decimalFormat.format(Float.valueOf(longitude));
								String spaces_key = key1+"_"+key2;
								
								//查询GET_DATA数据库中是否有此地址信息 否查询百度API
								String spaceGpsCass = "";
								Keyspace GPS_keyspace = HFactory.createKeyspace("GEO_DATA",cluster);
								SliceQuery<String, String, String> query = HFactory.createSliceQuery(GPS_keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
								query.setColumnFamily("geo_code");
								query.setKey(spaces_key);
								query.setColumnNames("city","district","province","street","street_number");
								
								//ColumnSliceIterator指column的名称值
								ColumnSliceIterator<String, String, String> iterator = new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);
								boolean hasValue = false;
								
								String province = "";
								String city = "";
								String district = "";
								String street = "";
								String street_number = "";
								while (iterator.hasNext()) {
									HColumn<String, String> element = iterator.next();
									if(element.getName().equals("province")){
										province = element.getValue();
									}else if(element.getName().equals("city")){
										city = element.getValue();
									}else if(element.getName().equals("district")){
										district = element.getValue();
									}else if(element.getName().equals("street")){
										street = element.getValue();
									}else if(element.getName().equals("street_number")){
										street_number = element.getValue();
									}
									System.out.println(element.getName() + ":" + element.getValue());
									System.out.println(spaces_key+" 初级命中缓存");
								}
								if(province.equals("")||province.equals("-")||province.equals("N/A")){
									hasValue = false;
								}else{
									hasValue = true;
								}
								
								spaceGpsCass = province+"_"+city+"_"+district+"_"+street+"_"+street_number;
								String spaceGps = "";
								String spaceGpsStr = "";
								if(hasValue){
									spaceGpsStr = spaceGpsCass;
									System.out.println(spaces_key+" 真实命中缓存" + spaceGpsStr);
								}else{
									String resp = testPost(latitude, longitude);
									spaceGpsStr = subLalo(resp);
									
									//插入GEO_DATA信息
									if(cluster.describeKeyspace("GEO_DATA")==null){
										createKeyspace("geo_code", "GEO_DATA");
									}
									Keyspace keyspace_geo = HFactory.createKeyspace("GEO_DATA", cluster);
									Mutator<String> spaceMutator = HFactory.createMutator(
											keyspace_geo, stringSerializer);
									String spaces[] = spaceGpsStr.split("_");
									String spaceName [] = new String[]{"province","city","district","street","street_number"};
									if(spaces.length==5){
										for (int j = 0; j < spaces.length; j++) {
											spaceMutator.addInsertion(spaces_key, "geo_code",
													HFactory.createStringColumn(spaceName[j],spaces[j]));
										}
										spaceMutator.execute();
									}
								}
								
								createKeyspace(cFtotalGps,keySpaceStr);
								createKeyspace(cFtotalReport,keySpaceStr);
								
								String spaces[] = spaceGpsStr.split("_");
								if(spaces.length==5){
									String spaceName1 [] = new String[]{"cassandra_province","cassandra_city","cassandra_district","cassandra_street","cassandra_street_number"};
									spaceGps = spaces[0]+"_"+spaces[1]+"_"+spaces[2];
									for (int j = 0; j < spaces.length; j++) {
										
										//同时修改原数据
										userMutator.addInsertion(row.getKey(), columnFamily,
												HFactory.createStringColumn(spaceName1[j],spaces[j]));
									}
									userMutator.addInsertion(row.getKey(), columnFamily,
											HFactory.createStringColumn("insert_state","14"));
									userMutator.execute();
								}
								
								//插入gps信息
								String keyStr = row.getKey();
								String yearMonthDay = getYearMonthDay(keyspace, columnFamily,keyStr);
								if(yearMonthDay!=null){
									spaceGps = spaceGps+"_"+yearMonthDay;
								}
								String value = queryExist(keyspace,cFtotalGps,columnFamily,spaceGps);
								System.out.println("key:"+spaceGps+",cf:"+cFtotalGps+",value_key"+columnFamily+",value:"+value);
								
								//插入省市县的信息
								String spaceName1 [] = new String[]{"province","city","district","street","street_number"};
								for (int j = 0; j < 3; j++) {
									userMutator.addInsertion(spaceGps, cFtotalGps,
											HFactory.createStringColumn(spaceName1[j],spaces[j]));
									userMutator.addInsertion(spaceGps, cFtotalReport,
											HFactory.createStringColumn(spaceName1[j],spaces[j]));
								}
								userMutator.addInsertion(spaceGps, cFtotalGps,
										HFactory.createStringColumn("year_month_day", yearMonthDay));//插入年月日的数据
								userMutator.addInsertion(spaceGps, cFtotalGps,
										HFactory.createStringColumn(columnFamily, (Integer.parseInt(value)+1)+""));	//插入统计信息
								
								userMutator.execute();
								
								//插入report信息
								if(row.getKey().endsWith("monitor.csv_0_0") || row.getKey().endsWith("abstract.csv_0") || row.getKey().endsWith("summary.csv_0")){
									String value1 = queryExist(keyspace,cFtotalReport,columnFamily,spaceGps);
									System.out.println("key:"+spaceGps+",cf:"+cFtotalReport+",value_key"+columnFamily+",value:"+value1);
									userMutator.addInsertion(spaceGps, cFtotalReport,
											HFactory.createStringColumn("year_month_day", yearMonthDay));//插入年月日的数据
									userMutator.addInsertion(spaceGps, cFtotalReport,
											HFactory.createStringColumn(columnFamily, (Integer.parseInt(value1)+1)+""));//插入统计信息
									userMutator.execute();
								}
							}
						}catch (HTimedOutException e) {
							e.printStackTrace();
						} catch (HectorException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		
	*/}
	
	public static String getYearMonthDay(Keyspace keyspace,String columnFamily,String keyStr){
		RangeSlicesQuery<String, String, String> rangeQuery = HFactory
		.createRangeSlicesQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);

		rangeQuery.setColumnFamily(columnFamily);
		rangeQuery.setKeys(keyStr,keyStr);//相同为只能获取一个key
		rangeQuery.setColumnNames("year_month_day");
		
		QueryResult<OrderedRows<String, String, String>> result = rangeQuery
				.execute();
		OrderedRows<String, String, String> oRows = result.get();
		List<Row<String, String, String>> rowList = oRows.getList();
		String timeStr =  "";
		for (Row<String, String, String> row : rowList) {
			System.out.println("key: " + row.getKey());
			List<HColumn<String, String>> columns = row.getColumnSlice()
					.getColumns();
			for (HColumn<String, String> hColumn : columns) {
				if(hColumn.getName().equals("year_month_day")){
					timeStr = hColumn.getValue();
				}
			}
		}
		if(timeStr==null || timeStr.trim().equals("")){
			timeStr = "UNKNOW";
		}
		return timeStr;
	}
	public static void insertCassandraDB(Map dataMap, String numType,
			File file, String keyspace,Integer i) {/*
		StringBuffer errStr3 = new StringBuffer("");
		try {
			createKeyspace(numType, keyspace);
			Set keySet = dataMap.keySet();
			Iterator iter = keySet.iterator();
			Keyspace keyspaceOperator = HFactory.createKeyspace(keyspace,
					cluster);
			Mutator<String> userMutator = HFactory.createMutator(
					keyspaceOperator, stringSerializer);
			String rowKey = (String) dataMap.get("file_index");
			while (iter.hasNext()) {
				String key = iter.next() + "";
				if (key == null || key.isEmpty()) {
					continue;
				}
				String value = (String) dataMap.get(key);
				System.out.println("key:" + key + ",value:" + value);
				userMutator.addInsertion(rowKey, numType,
						HFactory.createStringColumn(key, value));
			}
			userMutator.execute();
		}catch (HTimedOutException e) {
			//链接cassandra失败时重连
			if(i==3){
				System.out.println("请检查网络设置");
			}else
			{
				insertCassandraDB(dataMap, numType, file, keyspace ,++i);
			}
		} catch (HectorException e) {
			errStr3.append(file.getAbsolutePath() + ":\r\n");
			errStr3.append(e.toString() + "\r\n");
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileWriter fw;
		try {
			if(errStr3==null || errStr3.toString().trim().equals("")){
			}else{
				fw = new FileWriter(errFile, true);
				fw.write("\r\n" + new Date().toLocaleString() + "\r\n"
						+ errStr3.toString());
				fw.flush();
				fw.close();	
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	*/}

	public void copyErrFile(File filePath) {
		System.out.println("filePath:  " + filePath.getAbsolutePath());
		// File file = new File(ConfParser.errPath);
		// if(!file.exists()){
		// try {
		// file.mkdirs();
		// } catch (Exception e1) {
		// e1.printStackTrace();
		// }
		// }
		// copyFile(filePath, new
		// File(file.getAbsolutePath()+File.separator+filePath.getName()));
	}

	// 复制文件
	public void copyFile(String oldPath, String newPath) {
		try {
			int bytesum = 0;
			int byteread = 0;
			File oldfile = new File(oldPath);
			if (oldfile.exists()) { // 文件存在时
				InputStream inStream = new FileInputStream(oldPath); // 读入原文件
				FileOutputStream fs = new FileOutputStream(newPath);
				byte[] buffer = new byte[1444];
				int length;
				while ((byteread = inStream.read(buffer)) != -1) {
					bytesum += byteread; // 字节数 文件大小
					System.out.println(bytesum);
					fs.write(buffer, 0, byteread);
				}
				inStream.close();
			}
		} catch (Exception e) {
			System.out.println("复制单个文件操作出错");
			e.printStackTrace();

		}

	}

	private String convertFileSize(long fileS) {
		DecimalFormat df = new DecimalFormat("#.00");
		String fileSizeString = "";
		if (fileS < 1024) {
			fileSizeString = df.format((double) fileS) + "B";
		} else if (fileS < 1048576) {
			fileSizeString = df.format((double) fileS / 1024) + "K";
		} else if (fileS < 1073741824) {
			fileSizeString = df.format((double) fileS / 1048576) + "M";
		} else {
			fileSizeString = df.format((double) fileS / 1073741824) + "G";
		}
		return fileSizeString;
	}
	private String getLastModifies(File file){
		long modifiedTime = file.lastModified();
		Date date = new Date(modifiedTime);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:MM");
		String dd = sdf.format(date);
//		System.out.println("File name:" + file.getName() + " \tFile size: "
//				+ (double) ((double) fis.available() / 1024 / 1024) + "M"
//				+ " \tFile create Time: " + dd);
		return dd;
	}
	/**
	 * 单条单次语句处理
	 */
	/*private void setMapData(Map headerMap, Map bodyMap, Map testScenrioMap,
			String numType, File file, String[] dataLong, String keyspace,
			String imei) {
		System.out.println("开始第一步SetMapData************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		String filePath = file.getAbsolutePath().replace(ConfParser.org_prefix.replace(File.separator, "|"),"");
		String fileSize = convertFileSize(file.length());
		String file_lastModifies = getLastModifies(file);
		String fileName = file.getName();
//		String imsi = "";//获取IMSI
//		imsi = getIMSIFromDeviceInfo(file);
		Map allMap = new HashMap();
//		allMap.put("imsi", imsi);
		String detailreport = "";
		InsertMysqlByDataToGPS insertMysqlByDataToGPS = new InsertMysqlByDataToGPS();
		if(dataLong==null || dataLong.length==0){
			dataLong = new String[]{""};
		}
		if (bodyMap != null) {
			if (bodyMap.size() == 1) {
				Map bodyNextMap = (Map) bodyMap.get(1);
				String fileIndex = imei+"|"+file.getName()+ "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap = setMapData(bodyNextMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
//				insertCassandraDB(allMap, numType, file, keyspace,0);
				detailreport = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
//				insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
				
			} else if (bodyMap.size() > 1) {
				if(numType.equals("04002")){
					Map map = new HashMap();
					for (int i = 0; i < bodyMap.size(); i++) {
						Map lMap = new HashMap();
						Map bodyNextMap = (Map) bodyMap.get(i + 1);
						String fileIndex = imei+"|"+file.getName()+ "_" + i;
						lMap = setMapData(headerMap, numType, lMap, file,
								fileIndex, dataLong[i], imei);
						lMap = setMapData(testScenrioMap, numType, lMap, file,
								fileIndex, dataLong[i], imei);
						lMap = setMapData(bodyNextMap, numType, lMap, file,
								fileIndex, dataLong[i], imei);
						lMap.put("fileName", fileName);
						lMap.put("filePath", filePath);
						lMap.put("fileSize", fileSize);
						lMap.put("file_lastModifies", file_lastModifies);
						map.put(i, lMap);
					}
					map.put("flag", "1");
					detailreport = 	insertMysqlDao.insertMysqlDB(map, keyspace, numType,file,detailreport);
//					insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
				}else{
					for (int i = 0; i < bodyMap.size(); i++) {
						Map bodyNextMap = (Map) bodyMap.get(i + 1);
						String fileIndex = imei+"|"+file.getName()+ "_" + i;
						allMap = setMapData(headerMap, numType, allMap, file,
								fileIndex, dataLong[i], imei);
						allMap = setMapData(testScenrioMap, numType, allMap, file,
								fileIndex, dataLong[i], imei);
						allMap = setMapData(bodyNextMap, numType, allMap, file,
								fileIndex, dataLong[i], imei);
						allMap.put("fileName", fileName);
						allMap.put("filePath", filePath);
						allMap.put("fileSize", fileSize);
						allMap.put("file_lastModifies", file_lastModifies);
//						insertCassandraDB(lMap, numType, file, keyspace,0);
						detailreport = 	insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
//						insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
					}
				}
			}else{
				String fileIndex = imei+"|"+file.getName()+ "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file, fileIndex,
						dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
//				insertCassandraDB(allMap, numType, file, keyspace,0);
				detailreport = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
//				insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
			}
		} else {
			String fileIndex = imei+"|"+file.getName()+ "_" + 0;
			allMap = setMapData(headerMap, numType, allMap, file, fileIndex,
					dataLong[0], imei);
			allMap = setMapData(testScenrioMap, numType, allMap, file,
					fileIndex, dataLong[0], imei);
			allMap.put("fileName", fileName);
			allMap.put("filePath", filePath);
			allMap.put("fileSize", fileSize);
			allMap.put("file_lastModifies", file_lastModifies);
//			insertCassandraDB(allMap, numType, file, keyspace,0);
			detailreport = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
//			insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
		}
	}*/
	/**
	 * 单条多次语句处理
	 */
	private void setMapData(Map headerMap, Map bodyMap, Map testScenrioMap,
			String numType, File file, String[] dataLong, String keyspace,
			String imei) {
//		String oldNumType = "";
//		if(sqlList!=null && sqlList.size()>=1){
//			oldNumType = sqlList.get(0).toString();	
//		}else{
//			oldNumType = numType;
//			sqlList.add(oldNumType);
//		}
//		if(sqlList.size() >= 1000 || !oldNumType.equals(numType)){
//			dealData();			
//			sqlList.clear();
//			sqlList.add(numType);
//		}
		
		System.out.println("开始第一步SetMapData************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		List sqlList = new ArrayList();
		Map<StringBuffer, Boolean> resultMap = new HashMap<StringBuffer, Boolean>();
		String filePath = file.getAbsolutePath().replace(ConfParser.org_prefix.replace(File.separator, "|"),"");
		String fileSize = convertFileSize(file.length());
		String file_lastModifies = getLastModifies(file);
		String fileName = file.getName();
//		String imsi = "";//获取IMSI
//		imsi = getIMSIFromDeviceInfo(file);
		Map allMap = new HashMap();
//		allMap.put("imsi", imsi);
		String detailreport = "";
		InsertMysqlByDataToGPS insertMysqlByDataToGPS = new InsertMysqlByDataToGPS();
		if(dataLong==null || dataLong.length==0){
			dataLong = new String[]{""};
		}
		//获取detailreport
		String destFilePath = ConfParser.backReportPath;
		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
		String filenameorg = path1.substring(path1.lastIndexOf("."), path1
				.length());
		String[] files = new String[] { filenameorg, ".detail", ".deviceInfo",
				".monitor" ,".eventlogger"};
		String[] filepaths = new String[4];
		if (detailreport.equals("")) {
			for (int i = 0; i < filepaths.length; i++) {
				String newfilePath = file.getAbsolutePath().replace(
						filenameorg, files[i]);
				filepaths[i] = newfilePath;
				File newFile = new File(newfilePath);
				if (newFile.exists()) {
					detailreport += "1";
				} else {
					detailreport += "0";
				}
			}
		}
		//获取appid
		String appid = "";
		String appidStr = filePath.replace(ConfParser.org_prefix, "");
		while(appidStr.startsWith(File.separator)){
			appidStr = appidStr.substring(1, appidStr.length());
		}
		appidStr = appidStr.substring(0, appidStr.indexOf(File.separator));
		if (this.appid.contains(appidStr)) {
			appid = appidStr;
		}else{
			appid = "";
		}
		//处理数据
		if (bodyMap != null) {
			if (bodyMap.size() == 1) {
				Map bodyNextMap = (Map) bodyMap.get(1);
				String fileIndex = imei+"|"+file.getName()+ "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap = setMapData(bodyNextMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				allMap.put("appid", appid);
//				insertCassandraDB(allMap, numType, file, keyspace,0);
//				resultMap = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
				insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
//				insertSqlInfo(resultMap);
			} else if (bodyMap.size() > 1) {
//				if(numType.equals("04002")){
//					Map map = new HashMap();
//					for (int i = 0; i < bodyMap.size(); i++) {
//						Map lMap = new HashMap();
//						Map bodyNextMap = (Map) bodyMap.get(i + 1);
//						String fileIndex = imei+"|"+file.getName()+ "_" + i;
//						lMap = setMapData(headerMap, numType, lMap, file,
//								fileIndex, dataLong[i], imei);
//						lMap = setMapData(testScenrioMap, numType, lMap, file,
//								fileIndex, dataLong[i], imei);
//						lMap = setMapData(bodyNextMap, numType, lMap, file,
//								fileIndex, dataLong[i], imei);
//						lMap.put("fileName", fileName);
//						lMap.put("filePath", filePath);
//						lMap.put("fileSize", fileSize);
//						lMap.put("file_lastModifies", file_lastModifies);
//						lMap.put("appid", appid);
//						map.put(i, lMap);
//					}
//					map.put("flag", "1");
////					resultMap = insertMysqlDao.insertMysqlDB(map, keyspace, numType,file,detailreport);
//					insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
////					insertSqlInfo(resultMap);
//				}else{
					for (int i = 0; i < bodyMap.size(); i++) {
						Map bodyNextMap = (Map) bodyMap.get(i + 1);
						String fileIndex = imei+"|"+file.getName()+ "_" + i;
						allMap = setMapData(headerMap, numType, allMap, file,
								fileIndex, dataLong[i], imei);
						allMap = setMapData(testScenrioMap, numType, allMap, file,
								fileIndex, dataLong[i], imei);
						allMap = setMapData(bodyNextMap, numType, allMap, file,
								fileIndex, dataLong[i], imei);
						allMap.put("fileName", fileName);
						allMap.put("filePath", filePath);
						allMap.put("fileSize", fileSize);
						allMap.put("file_lastModifies", file_lastModifies);
						allMap.put("appid", appid);
//						insertCassandraDB(lMap, numType, file, keyspace,0);
//						resultMap = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
						insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
//						insertSqlInfo(resultMap);
					}
//				}
			}else{
				String fileIndex = imei+"|"+file.getName()+ "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file, fileIndex,
						dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				allMap.put("appid", appid);
//				insertCassandraDB(allMap, numType, file, keyspace,0);
//				resultMap = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
				insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
//				insertSqlInfo(resultMap);
			}
		} else {
			String fileIndex = imei+"|"+file.getName()+ "_" + 0;
			allMap = setMapData(headerMap, numType, allMap, file, fileIndex,
					dataLong[0], imei);
			allMap = setMapData(testScenrioMap, numType, allMap, file,
					fileIndex, dataLong[0], imei);
			allMap.put("fileName", fileName);
			allMap.put("filePath", filePath);
			allMap.put("fileSize", fileSize);
			allMap.put("file_lastModifies", file_lastModifies);
			allMap.put("appid", appid);
//			insertCassandraDB(allMap, numType, file, keyspace,0);
//			resultMap = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
			insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType, file, detailreport);
//			insertSqlInfo(resultMap);
		}
		String dataOrg = allMap.get("dataOrg")==null?"":allMap.get("dataOrg").toString();
		String dateYear = 1900+new Date().getYear()+"";
		String dateMonth = new Date().getMonth()+1+"";
		if(dateMonth.length()==1){
			dateMonth = "0"+dateMonth;
		}
		String dateNow = dateYear+dateMonth;
		dataOrg = dateNow;
		//备份数据
		System.out.println("结束插入数据  开始备份文件************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		for (int i = 0; i < filepaths.length; i++) {
			String newfilePath = filepaths[i];
			if (newfilePath == null || newfilePath.isEmpty()) {
				continue;
			}
			File destFile = new File(newfilePath.replace(
					ConfParser.org_prefix, destFilePath+dataOrg+File.separator));
			File newFile = new File(newfilePath);
			if (!newFile.exists()) {
				continue;
			}
			if (destFile.exists()) {
				destFile.delete();
			}
			if (!destFile.exists()) {
				if (!destFile.getParentFile().exists()) {
					destFile.getParentFile().mkdirs();
				}
				try {
					destFile.createNewFile();
					FileUtils.copyFile(newFile, destFile);
				} catch (IOException e) {
					e.printStackTrace();
				}finally{
					newFile.delete();
				}
			}
		}
		System.out.println("结束备份************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
	}
	
	private boolean insertData(Map resultMap){
		String sql = resultMap.keySet().iterator().next().toString();
		boolean flag = true;
		System.out.println("开始准备插入数据************："+new Date().toLocaleString() +"   毫秒："+new Date().getTime());
		if (sql.toString().isEmpty()) {
			flag = false;
		} else {
			try {
				start();
				insert(sql);
				flag = true;
			} catch (Exception e) {
				flag = false;
				System.out.println("执行sql语句错误");
				e.printStackTrace();
			} finally {
				try {
					close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return flag;
	}
	private void insertSqlInfo(Map resultMap){
		if(resultMap==null || resultMap.size()==0){
			return;
		}
		List sqlInfoList = new ArrayList();
		String sql = resultMap.keySet().iterator().next().toString();
		String flag = resultMap.values().iterator().next()+"";
		sqlInfoList.add(sql);
		sqlInfoList.add(flag);
		sqlList.add(sqlInfoList);
	}
	
	private void dealData(){
		String numType = sqlList.get(0).toString();
		String table = "";
		if(numType.equals("01001")){
			table = "speed_test";
		}else if(numType.equals("02001")){
			table = "web_browsing";
		}else if(numType.equals("02011")){
			table = "video_test";
		}else if(numType.equals("03001") || numType.equals("03011")){
			table = "http_test";
		}else if(numType.equals("04002")){
			table = "ping";
		}else if(numType.equals("04006")){
			table = "hand_over";
		}else if(numType.equals("05001")){
			table = "call_test";
		}else if(numType.equals("05002")){
			table = "sms";
		}else if(numType.equals("05005")){
			table = "sms_query";
		}else {
			return;
		}
		String tableStr = "insert into "+table+" ";
		String insertSql = "";
		String columnStr = "";
		String valuesStr = "";
		List updateList = new ArrayList();
		
		for (int i = 1; i < sqlList.size(); i++) {
			ArrayList list = (ArrayList) sqlList.get(i);
			String sql = (String) list.get(0);
			String flag = list.get(1)+"";
			if(!sql.isEmpty()){
				if(flag.equals("false")){
					if(sql.contains("values")){
						String insertStr = sql.replace(tableStr, "");
						if(columnStr.isEmpty()){
							columnStr = insertStr.substring(0,insertStr.indexOf("values")+6);
							insertSql += tableStr;
							insertSql += columnStr;
						}
						valuesStr = insertStr.substring(insertStr.indexOf("values")+6,insertStr.length()) +",";
						insertSql += valuesStr;
					}
				}else{
					updateList.add(sql);
				}
			}
		}
		while(insertSql.endsWith(",")){
			insertSql = insertSql.substring(0, insertSql.length()-1);
		}
		boolean flag = true;
		try {
			start();
			if(!insertSql.isEmpty()){
				System.out.println(insertSql);
				statement.execute(insertSql);
			}
			for (int i = 0; i < updateList.size(); i++) {
				String updateSql = updateList.get(i)==null?"":updateList.get(i).toString();
				if(updateSql.isEmpty()){
					continue;
				}
				System.out.println(updateSql);
				statement.execute(updateSql);
			}
		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("更新数据"+(flag==true?"成功":"失败"));
	}
	
	//目前为正常入app报告
	private Map setMapData(Map DataMap, String numType, Map allMap, File file,
			String fileIndex, String dataLong, String imei) {
		Set set = DataMap.keySet();
		Iterator iter = set.iterator();
		while (iter.hasNext()) {
			String key = (String) iter.next();
			String value = "";
			if (key == null || key.isEmpty()) {
				key = "";
			}
			try {
				value = (String) DataMap.get(key);
			} catch (Exception e) {
			}
			allMap.put(key, value);
		}
		String prefix = ConfParser.org_prefix.replace(File.separator, "|");
		fileIndex = fileIndex.replace(prefix, "");
		try {
			Date date = new Date(Long.valueOf(dataLong));	
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
			String dateStr = sdf.format(date);
			allMap.put("dataOrg", dateStr);
		} catch (Exception e) {
			System.out.println("年月日入库错误："+dataLong);
		}
//		System.out.println("fileIndex:"+fileIndex+",dataLong:"+dataLong+",yearMonthDay:"+dateStr+",imei:"+imei);
		allMap.put("file_index", fileIndex);
		allMap.put("time", dataLong);
		allMap.put("imei", imei);
		return allMap;
	}
	public String getIMSIFromDeviceInfo(File file){
		String filePath = file.getAbsolutePath();
		String deviceInfoPath = filePath.replace("summary", "deviceInfo");
		String imsi = "";
		File deviceFile = new File(deviceInfoPath);
		Map map = new HashMap();
		if(deviceFile.exists()){
			List<String[]> list = this.readCsv(deviceFile);
			for (int i = 0; i < list.size(); i++) {
				if(list.get(i)!=null){
					String key = list.get(i)[0];
					String value = "";
					if(list.get(i).length>=2){
						value = list.get(i)[1];
					}
					if(value!=null){
						map.put(key, value);
					}
				}
			}
		}
		if(map.containsKey("IMSI")){
			imsi = map.get("IMSI").toString();
		}
		return imsi;
	}
	public String subTxt(String str) {
		if (str.startsWith("{")) {
			str = str.substring(1, str.length());
		}
		if (str.endsWith("}")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}

	static public ResultSet queryById(int id) {
		String tableName = "road_test_info";
		String sql = "select * from " + tableName + " where id=" + id
				+ " and gpsInfoX!='N/A'";
		System.out.println("执行日志:sql语句详情>>>" + sql);
		Connection conn = null;
		PreparedStatement pst = null;
		try {
			conn = JDBCUtils.getConnection();

			pst = conn.prepareStatement(sql);

			return pst.executeQuery();

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, null);
		}
	}

	static public int[] queryByFilepath(String filePath) {
		int[] fileInfo = new int[2];
		fileInfo[0] = 0;
		fileInfo[1] = 0;

		ResultSet myRS = null;
		String tableName = "uploadfile_apply";
		String sql = "select * from " + tableName + " where store_path like '%"
				+ filePath + "%'";
		System.out.println("执行日志:sql语句详情>>>" + sql);
		Connection conn = null;
		PreparedStatement pst = null;
		try {
			conn = JDBCUtils.getConnection();

			pst = conn.prepareStatement(sql);

			myRS = pst.executeQuery();
			myRS.next();
			try {
				fileInfo[0] = myRS.getInt("id");
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				fileInfo[1] = myRS.getInt("org_id");
			} catch (Exception e) {
				e.printStackTrace();
			}
			return fileInfo;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			JDBCUtils.release(null, pst, null);
		}
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
			int cEnd = str.indexOf(",\"district");

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
	/**
	 * GPS坐标转换
	 * @param gps
	 * @return
	 */
	private static String getGpsPoint(String gps){
		String gpsPoint = "";
		if(gps.contains("°")){
			String degrees = gps.substring(0,gps.lastIndexOf("°"));
			String minutes = gps.substring(gps.lastIndexOf("°")+1,gps.lastIndexOf("′"));
			String seconds = gps.substring(gps.lastIndexOf("′")+1,gps.lastIndexOf("″"));
			
			//Long gpsLong = Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
			float gpsLong = Float.parseFloat(degrees)+Float.parseFloat(minutes)/60+Float.parseFloat(seconds)/3600;
			
			DecimalFormat decimalFormat=new DecimalFormat(".0000000");
			gpsPoint = decimalFormat.format(gpsLong);
		}else{
			if(gps.contains("E")){
				gpsPoint = gps.substring(0,gps.lastIndexOf("E"));
				float gpsLong = Float.valueOf(gpsPoint);
				DecimalFormat decimalFormat=new DecimalFormat(".0000000");
				gpsPoint = decimalFormat.format(gpsLong);
			}
			if(gps.contains("N")){
				gpsPoint = gps.substring(0,gps.lastIndexOf("N"));
				float gpsLong = Float.valueOf(gpsPoint);
				DecimalFormat decimalFormat=new DecimalFormat(".0000000");
				gpsPoint = decimalFormat.format(gpsLong);
			}
		}
		return gpsPoint;
	}
	
	/**
	 * GPS坐标转换
	 * @param meta
	 * @return
	 */
	private static String[] transGpsPoint(String meta){

		String[] result = new String[2];
		String[] info = null;
		if(meta!=null && !"".equals(meta)){
			info = meta.split(" ");
		}else{
			return null;
		}
		
		String latitude = "";
		String longitude = "";
		
		String gpsPoint = "";
		try{
			for(int i=0; i<info.length&&i<2; i++){
				if(info[i].contains("°")){
					String degrees = info[i].substring(0,info[i].lastIndexOf("°"));
					String minutes = info[i].substring(info[i].lastIndexOf("°")+1,info[i].lastIndexOf("′"));
					String seconds = info[i].substring(info[i].lastIndexOf("′")+1,info[i].lastIndexOf("″"));
					
					//Long gpsLong = Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
					float gpsLong = Float.parseFloat(degrees)+Float.parseFloat(minutes)/60+Float.parseFloat(seconds)/3600;
					
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if(info[i].contains("E")){
						longitude = gpsPoint;
					}else if(info[i].contains("N")){
						latitude = gpsPoint;
					}else if(gpsLong>80.0){
						longitude = gpsPoint;
					}else{
						latitude = gpsPoint;
					}
				}else{
					if(info[i].contains("E")){
						gpsPoint = info[i].substring(0,info[i].lastIndexOf("E"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					}else if(info[i].contains("N")){
						gpsPoint = info[i].substring(0,info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					}else{
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat=new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						if(gpsLong>80.0){
							longitude = gpsPoint;
						}else{
							latitude = gpsPoint;
						}
					}
				}
			}
			result[0] = longitude;
			result[1] = latitude;
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}
	
	public void start() throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		// String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
		String url = ConfParser.url;
		System.out.println("******************"+url);
		String user = ConfParser.user;
		// String password = "cmrictpdata";
		String password = ConfParser.password;
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		statement = conn.createStatement();
	}

	public void close() throws SQLException {
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
}
