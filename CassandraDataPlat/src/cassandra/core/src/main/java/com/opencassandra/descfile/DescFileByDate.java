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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import org.apache.commons.lang.StringUtils;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import com.csvreader.CsvReader;
import com.opencassandra.v01.dao.impl.InsertMysqlDao;

public class DescFileByDate {
	String root = "c:\\Orgnize\\";
	boolean isOverwrite = true;
	boolean isChange = false;
	static Map map = new HashMap();
	Map timeMap = new HashMap();
	static int mapCount = 0;
	static int length = 0;
	static List list_str = new ArrayList();
	static List list_datetime = new ArrayList();
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm",
			"ss", "SSS" };
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private static String ak = ConfParser.ak;
	int counts = 0;
	int errors = 0;
	public static int count = 0;
	private InsertMysqlDao insertMysqlDao = new InsertMysqlDao();

	public static int getCount() {
		return count;
	}

	synchronized public static void setCount() {
		DescFileByDate.count++;
	}

	public static int dataIndex = 256146;
	public static int successCount = 0;
	public static int failCount = 0;
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
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

	public DescFileByDate() {
		super();
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	public DescFileByDate(String root) {
		super();
		this.root = root;
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	/**
	 * @override
	 */
	public void run() {
		this.findFile();
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
		DescFileByDate desc = new DescFileByDate();
		SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> Start!");
		desc.setChange(true);
		String file[] =  new String[]{};
		file = ConfParser.srcReportPath;
		for (int i = 0; i < file.length; i++) {
			String filePath = file[i];
			System.out.println(filePath);
			desc.findFile(filePath);
		}
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

	public void findFile(String rootDirectory) {
		File rootFile = new File(rootDirectory);
		if(!rootFile.exists()){
			rootFile.mkdir();
		}
		File[] fileList = rootFile.listFiles();
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
					findFile(myFile.getAbsolutePath());
				}
				try {
					File filePath = new File(myFile.getAbsolutePath());
					try {
						this.deal(myFile, 01001);
					} catch (Exception e) {
						e.printStackTrace();
						errStr.append(myFile.getAbsolutePath() + ":\r\n");
						errStr.append(e.toString() + "\r\n");
						errors++;
						System.out.println("文件处理异常>>>"
								+ Paths.get(myFile.getAbsolutePath()));
						System.out.println("异常报告数为>>>" + errors);
					}
					counts++;
					if ((counts % 100) == 0) {
						System.out.println("已处理报告数为：" + length);
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

	public void findFile() {
		findFile(this.root);
	}

	public void deal(File file, int ServiceId) {
		if (file.getAbsoluteFile().toString().contains(".abstract.csv")
				|| file.getAbsoluteFile().toString().contains(".summary.csv")) {
			splitAbstractCsv(file, 01001);
			setCount();
		} else {
			return;
		}
	}

	public boolean splitAbstractCsv(File filePath, int ServiceId) {
		StringBuffer errStr1 = new StringBuffer("");
		try {
			String datetime = "";
			ArrayList<String[]> absHeader = new ArrayList<String[]>();
			ArrayList<String[]> absBody = new ArrayList<String[]>();
			ArrayList<String[]> testScenario = new ArrayList<String[]>();
			ArrayList<String[]> csvList = this.readCsv(filePath);
			int dataEreaIndex = 0;
			if (ServiceId == 01001) {
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
									|| csvList.get(i)[0].equals("Network 1Time")
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
						dataEreaIndex++;
					}
				}
			}
			this.writeToFile(absHeader, absBody, testScenario, filePath);
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
			File filePath) {
		String prefix = ConfParser.org_prefix;
		//从文件名中分解出的数据
		String numType = "";
		String imei = "";
		String fileName = "";//显示文件名
		String typeName = "";//类型名称
		String keyspace = "";//分组
		String model = "";//型号
		String mobile = "";
		String path = "";//显示路径
		String datatime[] = null;
		String data[] = filePath.getName().split("-_-");
		fileName = data[0];
		keyspace = data[1];
		typeName = data[2];
		mobile = data[3];
		model = data[4];
		imei = data[6];
		
//		keyspace = filePath.getAbsolutePath().replace(prefix, "");
//		keyspace = keyspace.substring(0, keyspace.indexOf(File.separator));
		path = keyspace+File.separator+typeName+File.separator+mobile+File.separator+model+File.separator+imei+File.separator+fileName;
		
		StringBuffer errStr2 = new StringBuffer("");
		// 获取测试类型Num
		Pattern pattern1 = Pattern.compile("[-_.]");
		Pattern pattern2 = Pattern.compile("\\d+");

		numType = pattern1.split(fileName)[1];
		
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
//					if ((key.contains("测试时间") || key.equals("测试开始时间")
//						||key.trim().toLowerCase().contains("time") 
//						|| key.trim().toLowerCase().equals("start time")
//						|| key.trim().toLowerCase().equals("test start time"))
//						) {
//							datatime[0] = value;
//						}
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
			absHeaderMap.put("测试类型Num", numType);
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
//								if ((key[0].contains("测试时间") || key[0].equals("测试开始时间")
//									||key[0].trim().toLowerCase().equals("time") || key[0].trim().toLowerCase().equals("start time") || key[0].trim().toLowerCase().equals("test start time") || key[0].trim().toLowerCase().equals("network 1time"))
//									) {
//										datatime[i-1] = value[0];
//									}
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
		
		try {
			//格式化时间
			String dataLong[] = new String[datatime.length];
//			for (int i = 0; i < datatime.length; i++) {
//				String datatimeStr = datatime[i];
//				Date date1 = null;
//				try {
//					if(datatimeStr==null || datatimeStr.isEmpty()){
//						dataLong[i] = "";	
//					}else{
//						String timeStrs[] = pattern2.split(datatimeStr);
//						StringBuffer rex = new StringBuffer("");
//						for (int j = 0; j < timeStrs.length; j++) {
//							rex.append(timeStrs[j]);
//							rex.append(dateStr[j]);
//						}
//						SimpleDateFormat formatter = new SimpleDateFormat(
//								rex.toString());
//						date1 = formatter.parse(datatimeStr);	
//						dataLong[i] = Long.toString(date1.getTime());	
//					}
//					
//				} catch (ParseException e) {
//					e.printStackTrace();
//					System.out.println("转换时间有误");
//				}
//			}
			
			setMapData(absHeaderMap, absBodyMap, testScenarioMap, numType,
					filePath, dataLong, keyspace, imei, path ,fileName);

		} catch (Exception e) {
			copyErrFile(filePath);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

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
	private void setMapData(Map headerMap, Map bodyMap, Map testScenrioMap,
			String numType, File file, String dataLong[], String keyspace,
			String imei,String Path,String fileRealName) {
		String filePath = file.getAbsolutePath().replace(ConfParser.org_prefix.replace(File.separator, "|"),"");
		String fileSize = convertFileSize(file.length());
		String file_lastModifies = getLastModifies(file);
		String fileName = file.getName();
		String detailreport = "";
		boolean flag = false;
		Map allMap = new HashMap();
		if (bodyMap != null) {
			if (bodyMap.size() == 1) {
				Map bodyNextMap = (Map) bodyMap.get(1);
				String fileIndex = imei+"|"+fileRealName+ "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap = setMapData(bodyNextMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap.put("pathToShow", Path);
				allMap.put("nameToShow", fileRealName);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
//				insertCassandraDB(allMap, numType, file, keyspace,0);
			//	detailreport = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
			} else if (bodyMap.size() > 1) {
				for (int i = 0; i < bodyMap.size(); i++) {
					Map lMap = new HashMap();
					Map bodyNextMap = (Map) bodyMap.get(i + 1);
					String fileIndex = imei+"|"+fileRealName+ "_" + i;
					lMap = setMapData(headerMap, numType, allMap, file,
							fileIndex, dataLong[i], imei);
					lMap = setMapData(testScenrioMap, numType, allMap, file,
							fileIndex, dataLong[i], imei);
					lMap = setMapData(bodyNextMap, numType, allMap, file,
							fileIndex, dataLong[i], imei);
					allMap.put("pathToShow", Path);
					allMap.put("nameToShow", fileRealName);
					allMap.put("fileName", fileName);
					allMap.put("filePath", filePath);
					allMap.put("fileSize", fileSize);
					allMap.put("file_lastModifies", file_lastModifies);
//					insertCassandraDB(lMap, numType, file, keyspace,0);
					//detailreport = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
				}
			}else{
				String fileIndex = imei+"|"+fileRealName+ "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file, fileIndex,
						dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file,
						fileIndex, dataLong[0], imei);
				allMap.put("pathToShow", Path);
				allMap.put("nameToShow", fileRealName);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
//				insertCassandraDB(allMap, numType, file, keyspace,0);
				//detailreport = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
			}
		} else {
			String fileIndex = imei+"|"+fileRealName+ "_" + 0;
			allMap = setMapData(headerMap, numType, allMap, file, fileIndex,
					dataLong[0], imei);
			allMap = setMapData(testScenrioMap, numType, allMap, file,
					fileIndex, dataLong[0], imei);
			allMap.put("pathToShow", Path);
			allMap.put("nameToShow", fileRealName);
			allMap.put("fileName", fileName);
			allMap.put("filePath", filePath);
			allMap.put("fileSize", fileSize);
			allMap.put("file_lastModifies", file_lastModifies);
//			insertCassandraDB(allMap, numType, file, keyspace,0);
			//detailreport = insertMysqlDao.insertMysqlDB(allMap, keyspace, numType,file,detailreport);
		}
		System.out.println("更新数据："+flag);
	}

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
		Date date = new Date(Long.valueOf(dataLong));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String dateStr = sdf.format(date);
//		System.out.println("fileIndex:"+fileIndex+",dataLong:"+dataLong+",yearMonthDay:"+dateStr+",imei:"+imei);
		allMap.put("file_index", fileIndex);
		allMap.put("data_time", dataLong);
		allMap.put("year_month_day", dateStr);
		allMap.put("imei", imei);
		return allMap;
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
}
