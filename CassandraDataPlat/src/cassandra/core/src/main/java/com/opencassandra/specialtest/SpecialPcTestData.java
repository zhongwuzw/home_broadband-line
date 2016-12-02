package com.opencassandra.specialtest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.text.DecimalFormat;
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

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.csvreader.CsvReader;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.descfile.GetPostTest;
import com.opencassandra.service.utils.UtilDate;

/**
 * 对pc报告的解析   该程序只解析文件的第一部分，并对所有的字段进行数据入库
 * 
 * @author：kxc
 * @date：Mar 16, 2016
 */
public class SpecialPcTestData {
	String root = "c:\\Orgnize\\";
	boolean isOverwrite = true;
	boolean isChange = false;
	static Map map = new HashMap();
	Map timeMap = new HashMap();
	static int mapCount = 0;
	static int length = 0;
	static List list_str = new ArrayList();
	static List list_datetime = new ArrayList();
	private String[] dateStr = new String[] { "yyyy", "MM", "dd", "HH", "mm", "ss", "SSS" };
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private static String ak = ConfParser.ak;
	int counts = 0;
	int errors = 0;
	public static int count = 0;
	private static SpecialPcTestDao insertMysqlDaoByPc = new SpecialPcTestDao();
	private List appid = new ArrayList();
	private static String pathType = ConfParser.pathType;

	// 截取报告入库时间的rex
	private static Pattern pattern1 = Pattern.compile("[_]");
	private static Pattern pattern2 = Pattern.compile("[.]");

	static String basename = "";// 可以给一个默认的库名，即使是没有code值也可以入到默认库中而不至于使程序报错

	public static int getCount() {
		return count;
	}

	synchronized public static void setCount() {
		SpecialPcTestData.count++;
	}

	public static int dataIndex = 256146;
	public static int successCount = 0;
	public static int failCount = 0;
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
	private ExecutorService analyzeThreadPool;

	static String srcPath = "\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.]\\d+[-_.].*";

	// static String destPathPattern = "ORGKEY/YEAR_MONTH/DAY";
	static String destPathPattern = "ORGKEY/SERVICETYPE/TERMINALTYPE/TERMINALMODEL/IMEI";

	static String regexName = "[^_.]+";
	// static String regexName = "[^_.-]+";
	static String regexPath = "[^/\\\\]+";

	/**
	 * 读取CSV文件
	 * 
	 * @param filePath
	 *            要解析的文件
	 * @return 返回一个装有string数组的list csv文件读取是以行为单位进行的
	 */
	public ArrayList<String[]> readCsv(File filePath) {
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath.getAbsolutePath();
			CsvReader reader = new CsvReader(csvFilePath, ',', Charset.forName("GB2312")); // 一般用这编码读就可以了

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
		if (sourceData != null && sourceData.contains("°") && sourceData.contains("′") && sourceData.contains("″")) {

			// 将输入的字符串分成两截，分别为度数du_string，分数fen_string
			String du_string = sourceData.substring(0, sourceData.indexOf("°"));
			// substring方法两个参数遵循规则"前包含后不包含"，例，”1234“.substring(0,2)返回的结果是index为0到1的子串，即"12"
			String fen_string = sourceData.substring(sourceData.indexOf("°") + 1, sourceData.indexOf("′"));
			// substring方法两个参数遵循规则"前包含后不包含"，例，”1234“.substring(0,2)返回的结果是index为0到1的子串，即"12"
			String miao_string = sourceData.substring(sourceData.indexOf("′") + 1, sourceData.indexOf("″"));

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
	 * 
	 * @param fileAttribute
	 * @param filePath
	 * @return
	 * @return Map<String,String>
	 */
	public Map<String, String> splitDeviceInfoCsv2(Map<String, String> fileAttribute, File filePath) {
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
		fileAttribute.put("mfr", deviceInfoTerminal.get(5)[1] != null ? deviceInfoTerminal.get(5)[1] : "");
		fileAttribute.put("sim_state", deviceInfoNetwork.get(2)[1] != null ? deviceInfoNetwork.get(2)[1] : "");
		fileAttribute.put("net_state", deviceInfoNetwork.get(4)[1] != null ? deviceInfoNetwork.get(4)[1] : "");
		fileAttribute.put("operator", deviceInfoNetwork.get(0)[1] != null ? deviceInfoNetwork.get(0)[1] : "");
		fileAttribute.put("roaming", deviceInfoNetwork.get(5)[1] != null ? deviceInfoNetwork.get(5)[1] : "");
		fileAttribute.put("inside_ip", deviceInfoTestinfo.get(4)[1] != null ? deviceInfoTestinfo.get(4)[1] : "");
		fileAttribute.put("outside_ip", deviceInfoTestinfo.get(5)[1] != null ? deviceInfoTestinfo.get(5)[1] : "");
		fileAttribute.put("mac", deviceInfoTestinfo.get(6)[1] != null ? deviceInfoTestinfo.get(6)[1] : "");
		fileAttribute.put("l_net_type", deviceInfoTestinfo.get(1)[1] != null ? deviceInfoTestinfo.get(1)[1] : "");
		fileAttribute.put("l_net_name", deviceInfoTestinfo.get(2)[1] != null ? deviceInfoTestinfo.get(2)[1] : "");
		fileAttribute.put("os_ver", deviceInfoTerminal.get(3)[1] != null ? deviceInfoTerminal.get(3)[1] : "");
		fileAttribute.put("ots_ver", deviceInfoTerminal.get(4)[1] != null ? deviceInfoTerminal.get(4)[1] : "");
		fileAttribute.put("machine_model", deviceInfoTerminal.get(7)[1] != null ? deviceInfoTerminal.get(7)[1] : "");
		fileAttribute.put("hw_ver", deviceInfoTerminal.get(10)[1] != null ? deviceInfoTerminal.get(10)[1] : "");
		fileAttribute.put("ots_test_position", deviceInfoTestinfo.get(0)[1] != null ? deviceInfoTestinfo.get(0)[1] : "");
		fileAttribute.put("server_info", deviceInfoTestinfo.get(3)[1] != null ? deviceInfoTestinfo.get(3)[1] : "");

		return fileAttribute;
	}

	/**
	 * 修改文件路径进行文件备份
	 * 
	 * @param file
	 * @param fileAttributes
	 * @return
	 * @return boolean
	 */
	public boolean changePath(File file, Map<String, String> fileAttributes) {
		if (!file.getAbsolutePath().contains(".abstract.csv")) {
			return false;
		}
		List<String> destPath = destPathSpilt();
		String destFilePath = "";
		if (destPath != null && destPath.size() > 0) {
			for (int i = 0; i < destPath.size(); i++) {
				if (!destPath.get(i).contains("_")) {
					destFilePath += fileAttributes.get(destPath.get(i)) + File.separator;
				} else {
					String[] strs = destPath.get(i).split("_");
					for (int j = 0; j < strs.length; j++) {
						destFilePath += fileAttributes.get(strs[j]) + "_";
					}
					destFilePath = destFilePath.substring(0, destFilePath.length() - 1) + File.separator;
				}
			}
		}

		destFilePath = this.root + destFilePath;
		File destFile = new File(destFilePath + fileAttributes.get("realfilename"));
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
				System.out.println("目标文件已存在，已覆盖: " + destFile.getAbsolutePath());
				// return true;
			}
		}
		File deviceFile = new File(file.getAbsolutePath().replace(".abstract.csv", ".deviceInfo.csv"));
		if (deviceFile.exists()) {
			File destDeviceFile = new File(destFilePath + fileAttributes.get("realfilename").replace(".abstract.csv", ".deviceInfo.csv"));
			if (!destDeviceFile.exists()) {
				if (this.copyFile(deviceFile, destDeviceFile)) {
					System.out.println("成功生成目标文件: " + destDeviceFile.getAbsolutePath());
				} else {
					return false;
				}
			} else if (destDeviceFile.exists() && !isOverwrite) {
				System.out.println("目标文件已存在，未覆盖: " + destDeviceFile.getAbsolutePath());
				// return false;
			} else {
				if (this.copyFile(deviceFile, destDeviceFile)) {
					System.out.println("目标文件已存在，已覆盖: " + destDeviceFile.getAbsolutePath());
					// return true;
				}
				deviceFile.renameTo(new File(deviceFile.getAbsoluteFile() + ".bak"));
			}
		}

		File monitorFile = new File(file.getAbsolutePath().replace(".abstract.csv", ".monitor.csv"));
		if (monitorFile.exists()) {
			File destMonitorFile = new File(destFilePath + fileAttributes.get("realfilename").replace(".abstract.csv", ".monitor.csv"));
			if (!destMonitorFile.exists()) {
				if (this.copyFile(monitorFile, destMonitorFile)) {
					System.out.println("成功生成目标文件: " + destMonitorFile.getAbsolutePath());
				} else {
					return false;
				}
			} else if (destMonitorFile.exists() && !isOverwrite) {
				System.out.println("目标文件已存在，未覆盖: " + destMonitorFile.getAbsolutePath());
				// return false;
			} else {
				if (this.copyFile(monitorFile, destMonitorFile)) {
					System.out.println("目标文件已存在，已覆盖: " + destMonitorFile.getAbsolutePath());
					// return true;
				}
				monitorFile.renameTo(new File(monitorFile.getAbsoluteFile() + ".bak"));
			}
		}

		File detailFile = new File(file.getAbsolutePath().replace(".abstract.csv", ".detail.csv"));
		if (detailFile.exists()) {
			File destDetailFile = new File(destFilePath + fileAttributes.get("realfilename").replace(".abstract.csv", ".detail.csv"));
			if (!destDetailFile.exists()) {
				if (this.copyFile(detailFile, destDetailFile)) {
					System.out.println("成功生成目标文件: " + destDetailFile.getAbsolutePath());
					return true;
				}
			} else if (destDetailFile.exists() && !isOverwrite) {
				System.out.println("目标文件已存在，未覆盖: " + destDetailFile.getAbsolutePath());
				return false;
			} else {
				if (this.copyFile(detailFile, destDetailFile)) {
					System.out.println("目标文件已存在，已覆盖: " + destDetailFile.getAbsolutePath());
					return true;
				}
				detailFile.renameTo(new File(detailFile.getAbsoluteFile() + ".bak"));
			}
		}
		return false;
	}

	/**
	 * 进行文件备份
	 * 
	 * @param srcFile
	 * @param destFile
	 * @return
	 * @return boolean
	 */
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

	/**
	 * 文件解析的主方法
	 * 
	 * @param args
	 * @return void
	 */
	static public void main(String[] args) {
		SpecialPcTestData desc = new SpecialPcTestData();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> Start!");
		desc.setChange(true);
		String file[] = new String[] {};
		desc.appid = desc.getAppids();// 获取匹配的文件的下级目录名
		file = ConfParser.srcReportPath;// 获取要解析文件的根目录 就是appid文件的上级目录
		//String dataname = ConfParser.dsturl.substring(ConfParser.dsturl.lastIndexOf("/")+1,ConfParser.dsturl.length());
		String dataname = ConfParser.subdatabase;

		if (pathType.equals("default")) {
			/**
			 * 遍历要所有解析文件根目录的
			 */
			for (int i = 0; i < file.length; i++) {
				String filePath = file[i];
				System.out.println(filePath);
				String org = "";
				String filePathOrg = "";
				if (filePath.endsWith("Default")) {
					filePathOrg = filePath.replace(File.separator + "Default", "");
				}
				if (filePathOrg.contains(File.separator)) {
					org = filePathOrg.substring(filePathOrg.lastIndexOf(File.separator) + 1, filePathOrg.length());
				} else {
					continue;
				}
				desc.findFile(filePath, org);
			}
		} else {
			for (int i = 0; i < file.length; i++) {
				String filePath = file[i];
				System.out.println(filePath);
				String org = "";
				String filePathOrg = "";
				while (filePath.endsWith(File.separator)) {
					filePath = filePath.substring(0, filePath.length() - 1);
				}
				org = filePath.substring(filePath.lastIndexOf(File.separator) + 1, filePath.length());
				desc.findFile(filePath, org);
			}
		}

		GetPostTest getPostTest = new GetPostTest();
		String codeUrl = ConfParser.getCodePath;
		if (pathType.equals("default")) {
			/**
			 * 层级结构的路径 ： 需调用带有Default逻辑的方法
			 */
			if (codeUrl.isEmpty()) {
				// 根据获取的appid进行文件路径的拼接 (新的文件命名规则的路径拼接)
				String[] paths = getPostTest.getPathsDefault(desc.appid);

				// 循环取出所有的要解析的文件的路径 如 ： 直到default
				// C:\test\my\1708467f00836880517dad80e17b1d8c\CMCC\CMCC_GUANGXI\CMCC_GUANGXI_PINZHIBU23G\CMCC_GUANGXI_PINZHIBU23G_NN\Default
				for (int i = paths.length - 1; i >= 0; i--) {
					String filePath = paths[i];
					if (filePath == null || filePath.isEmpty()) {
						continue;
					}
					System.out.println(filePath + "细分层的上层路径");
					String org = "";
					String filePathOrg = "";
					if (filePath.endsWith("Default")) {
						filePathOrg = filePath.replace(File.separator + "Default", "");
					}
					if (filePathOrg.contains(File.separator)) {
						org = filePathOrg.substring(filePathOrg.lastIndexOf(File.separator) + 1, filePathOrg.length());
					}
					// 根据文件的绝对路径以及org(CMCC_GUANGXI_PINZHIBU23G_NN)来进行文件夹内的文件遍历获取最终需解析文件
					desc.findFile(filePath, org);
				}
			} else {
				String dbName = "";
				for (int j = 0; j < ConfParser.code.length; j++) {
					ArrayList keyList = new ArrayList();
					String code2 = ConfParser.code[j];
					if (code2 == null || code2.isEmpty()) {
						continue;
					}
					String codeStr = "code=" + code2;
					System.out.println(ConfParser.url);

					/**
					 * 2015年12月11日 16:01:39修改 添加是否分表开关 若issubmeter=no
					 * 则不分表，其他情况都视为分表情况
					 */
					if (ConfParser.issubmeter.equals("no")) {
						basename = dataname;
					} else {
						basename = insertMysqlDaoByPc.getBaseName(code2);

						if (basename.isEmpty()) {
							basename = dataname;
						}

					}
					insertMysqlDaoByPc.startOrCreateDB(basename);

					if (code2.equals("all")) {
						String paths = "";
						if (ConfParser.org_prefix != null && !ConfParser.org_prefix.isEmpty()) {
							paths = ConfParser.org_prefix;
						} else {
							if (ConfParser.srcReportPath != null && ConfParser.srcReportPath.length > 0 && ConfParser.srcReportPath[0] != null) {
								paths = ConfParser.srcReportPath[0];
							}
						}
						File filePaths = new File(paths);
						File files[] = filePaths.listFiles(new FilenameFilter() {
							public boolean accept(File dir, String name) {
								if (dir.isDirectory()) {
									return true;
								}
								return false;
							}
						});
						for (int i = 0; i < files.length; i++) {
							File rootFile = files[i];
							String rootFileOrg = rootFile.getAbsolutePath().replace(ConfParser.org_prefix, "");
							if (rootFileOrg.endsWith(File.separator)) {
								rootFileOrg = rootFileOrg.substring(0, rootFileOrg.lastIndexOf(File.separator));
							}
							desc.findFile(rootFile.getAbsolutePath(), rootFileOrg);
						}
					} else {
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
								storePathSelf = storePathSelf.replace("/", File.separator).replace("\\", File.separator);
								if (storePathSelf.endsWith("Default")) {
									storePathSelf = storePathSelf.replace(File.separator + "Default", "");
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
								filePathOrg = filePath.replace(File.separator + "Default", "");
							}
							if (filePathOrg.contains(File.separator)) {
								org = filePathOrg.substring(filePathOrg.lastIndexOf(File.separator) + 1, filePathOrg.length());
							}
							// org =
							// filePath.substring(filePath.lastIndexOf(File.separator)+1,
							// filePath.length());
							desc.findFile(filePath, org);
						}
					}

				}
			}
		} else {
			/**
			 * 不带Default的解析路径获取
			 */
			if (codeUrl.isEmpty()) {
				String[] paths = getPostTest.getPaths(desc.appid);
				for (int i = paths.length - 1; i >= 0; i--) {
					String filePath = paths[i];
					if (filePath == null || filePath.isEmpty()) {
						continue;
					}
					System.out.println(filePath);
					String org = "";
					while (filePath.endsWith(File.separator)) {
						filePath = filePath.substring(0, filePath.length() - 1);
					}
					org = filePath.substring(filePath.lastIndexOf(File.separator) + 1, filePath.length());
					desc.findFile(filePath, org);
				}
			} else {
				String[] paths = getPostTest.getPaths(desc.appid);
				for (int i = paths.length - 1; i >= 0; i--) {
					String filePath = paths[i];
					if (filePath == null || filePath.isEmpty()) {
						continue;
					}
					System.out.println(filePath);
					String org = "";
					while (filePath.endsWith(File.separator)) {
						filePath = filePath.substring(0, filePath.length() - 1);
					}
					org = filePath.substring(filePath.lastIndexOf(File.separator) + 1, filePath.length());
					desc.findFile(filePath, org);
				}
			}
		}
		System.out.println("counts: " + desc.counts);
		System.out.println("successCount: " + successCount);
		System.out.println("failCount: " + failCount);
		desc.getAnalyzeThreadPool().shutdown();
		dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> End!");
		map.put("存储路径", mapCount++);

	}

	/**
	 * 根据文件配置appname获取查询出的appid用以确定文件下级目录名
	 * 
	 * @return
	 * @return List appid
	 */
	public List getAppids() {
		List appids = new ArrayList();
		String[] appnames = ConfParser.appname;
		boolean flag = false;
		String names = "";
		if (appnames != null && appnames.length > 0) {
			names = "(";
			for (int i = 0; i < appnames.length; i++) {
				String appname = appnames[i];
				if (appname.equals("OTS APP 1.5系列") || appname.equals("OTS 1.5 for Android")) {
					flag = true;
					continue;
				} else {
					if (!appname.isEmpty()) {
						names += "'" + appname + "',";
					}
				}
			}
			while (names.endsWith(",")) {
				names = names.substring(0, names.length() - 1);
			}
			names += ")";
		}
		String sql = "";
		if (names.isEmpty() || names.equals("()")) {
			if (flag) {
				appids.add("default_app_id");
			}
			return appids;
		} else {
			sql = "SELECT app_id FROM b_appid_info where app_name_ch in " + names + " or app_name_en in " + names + ";";
			appids = insertMysqlDaoByPc.getappIds(sql);
			if (flag) {
				appids.add("default_app_id");
			}
			return appids;
		}
	}

	/**
	 * 根据文件的绝对路径以及org(CMCC_GUANGXI_PINZHIBU23G_NN)来进行文件夹内的文件遍历获取最终需解析文件
	 * 
	 * @param rootDirectory
	 * @param org
	 * @return void
	 */
	public void findFile(String rootDirectory, String org) {
		File rootFile = new File(rootDirectory);
		if (!rootFile.exists()) {
			return;
		}
		File[] fileList = rootFile.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				if (dir.isDirectory() || name.indexOf(".summary.csv") >= 0 || name.indexOf(".abstract.csv") >= 0) {
					return true;
				}
				return false;
			}
		});
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuffer errStr = new StringBuffer();
		try {
			for (int i = 0; i < fileList.length; i++) {
				File myFile = fileList[i];
				if (myFile.isDirectory()) {
					length = length + myFile.listFiles().length;
					findFile(myFile.getAbsolutePath(), org);
				}
				try {
					try {
						// 判断文件格式并解析
						this.deal(myFile, 01001, org);
					} catch (Exception e) {
						e.printStackTrace();
						errStr.append(myFile.getAbsolutePath() + ":\r\n");
						errStr.append(e.toString() + "\r\n");
						errors++;
						System.out.println("文件处理异常>>>" + Paths.get(myFile.getAbsolutePath()));
						System.out.println("异常报告数为>>>" + errors);
					}
					counts++;
					if ((counts % 100) == 0) {
						// System.out.println("已处理报告数为：" + length);
						// System.out.println("已处理报告数为："+counts+"/"+nums);
					}
				} catch (Exception e) {
					e.printStackTrace();
					errStr.append(myFile.getAbsolutePath() + ":\r\n");
					errStr.append(e.getMessage() + "\r\n");
				}
			}
			Iterator iter = timeMap.keySet().iterator();
			// System.out.println(dateString + ">>> 发现待处理文件数量为： " + length);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * 判断文件格式，并解析文件
	 * 
	 * @param file
	 *            要判断的文件
	 * @param ServiceId
	 * @param org
	 */
	public void deal(File file, int ServiceId, String org) {
		if (file.getAbsoluteFile().toString().endsWith(".abstract.csv") || file.getAbsoluteFile().toString().endsWith(".summary.csv")) {

			// 解析文件
			splitAbstractCsv(file, 01001, org);
			setCount();
		} else {
			return;
		}
	}

	/**
	 * 解析文件
	 * 
	 * @param filePath
	 *            特定格式的文件
	 * @param ServiceId
	 * @param org
	 * @return
	 */
	public boolean splitAbstractCsv(File filePath, int ServiceId, String org) {
		StringBuffer errStr1 = new StringBuffer("");
		try {
			ArrayList<String[]> csvHeader = new ArrayList<String[]>();// 文头
			// ArrayList<String[]> csvBody = new ArrayList<String[]>();// 文体

			// 读取要解析的文件
			ArrayList<String[]> csvList = this.readCsv(filePath);
			int dataEreaIndex = 0;
			if (ServiceId == 01001) {
				if (csvList == null || csvList.size() == 0) {
					return false;
				}
				for (int i = 0; i < csvList.size(); i++) {
					if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
						if (csvList.get(i)[0] != null) {
							if (csvList.get(i)[0].equals("测试开始") && csvList.size() > i + 1 && csvList.get(i + 1) != null && csvList.get(i + 1).length >= 2 && csvList.get(i + 1)[0] != null
									&& csvList.get(i + 1)[0].contains("测试结束")) {

							} else if (csvList.get(i)[0].equals("业务类型") || csvList.get(i)[0].equals("序号") || csvList.get(i)[0].equals("目标地址") || csvList.get(i)[0].equals("协议类型")
									|| csvList.get(i)[0].equals("测速类型") && csvList.get(i).length > 2 && csvList.get(i)[2] != null && !csvList.get(i)[2].equals("")) {
								dataEreaIndex = 1;
								// } else if
								// (csvList.get(i)[0].contains("Test Scenario"))
								// {
								// dataEreaIndex = 2;
							}
						}
						if (dataEreaIndex == 0) {
							csvHeader.add(csvList.get(i));
						} /*
						 * else if (dataEreaIndex == 1) {
						 * csvBody.add(csvList.get(i)); // } else if
						 * (dataEreaIndex == 2) { //
						 * testScenario.add(csvList.get(i)); }
						 */
					}
				}
			}
			this.writeToFile(csvHeader, filePath, org);
		} catch (Exception e) {
			copyErrFile(filePath);
			errStr1.append(filePath.getAbsolutePath() + ":\r\n");
			errStr1.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 把数据放入map中，并且进行文件的备份
	 * 
	 * @param absHeader
	 * @param filePath
	 * @param org
	 * @return void
	 */
	private void writeToFile(ArrayList<String[]> absHeader, File filePath, String org) {
		StringBuffer errStr2 = new StringBuffer("");
		// 获取测试类型Num
		Pattern pattern1 = Pattern.compile("[-_.]");
		String numStrs[] = pattern1.split(filePath.getName());
		/*
		 * if (absBody == null || absBody.size() == 0) { datatime = new
		 * String[1]; } else { datatime = new String[absBody.size()]; }
		 */
		// 要生成的TXT的文件内容
		StringBuffer str = new StringBuffer();
		// 头部Map
		Map absHeaderMap = new HashMap();
		// 主体Map
		// Map absBodyMap = new HashMap();
		try {
			for (int i = 0; i < absHeader.size(); i++) {
				String key = absHeader.get(i)[0];
				try {
					String value = "";
					if (absHeader.get(i).length >= 2) {
						value = absHeader.get(i)[1];
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

		} catch (Exception e) {
			copyErrFile(filePath);
			errStr2.append(filePath.getAbsolutePath() + ":\r\n");
			errStr2.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		try {
			setMapData(absHeaderMap, numStrs[1], filePath.getAbsoluteFile(), org);
		} catch (Exception e) {
			copyErrFile(filePath);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void copyErrFile(File filePath) {
		System.out.println("filePath:  " + filePath.getAbsolutePath());
	}

	// 复制文件

	/**
	 * 解析文件的大小
	 * 
	 * @param fileS
	 * @return
	 */
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

	/**
	 * 获取解析文件的最近一次的修改时间
	 * 
	 * @param file
	 * @return
	 */
	private String getLastModifies(File file) {
		long modifiedTime = file.lastModified();
		Date date = new Date(modifiedTime);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:MM");
		String dd = sdf.format(date);
		// System.out.println("File name:" + file.getName() + " \tFile size: "
		// + (double) ((double) fis.available() / 1024 / 1024) + "M"
		// + " \tFile create Time: " + dd);
		return dd;
	}

	/**
	 * 把数据按测试类型分类进行处理
	 * 
	 * @param headerMap
	 * @param bodyMap
	 * @param numType
	 * @param file
	 * @param org
	 */
	private void setMapData(Map headerMap, String numType, File file, String org) {
		// 解析文件的绝对路径
		String filePath = file.getAbsolutePath().replace(ConfParser.org_prefix.replace(File.separator, "|"), "");
		// 获取解析文件的大小
		String fileSize = convertFileSize(file.length());
		// 获取文件最近一次的修改时间
		String file_lastModifies = getLastModifies(file);
		// 解析文件名
		String fileName = file.getName();
		Map allMap = new HashMap();
		boolean flag = false;
		String destFilePath = ConfParser.backReportPath;
		// 要解析的文件名 00000000_04002.000_0-2015_08_18_16_21_46_655.summary.csv
		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
		// 截取文件的标识格式summary
		String filenameorg = path1.substring(path1.lastIndexOf("."), path1.length());
		// 文件标识格式
		String[] files = new String[] { filenameorg, ".detail", ".deviceInfo", ".monitor" };
		// 文件备份的标识格式
		String[] filepaths = new String[4];
		for (int i = 0; i < files.length; i++) {
			String newfilePath = file.getAbsolutePath().replace(filenameorg, files[i]);
			filepaths[i] = newfilePath;
		}
		String mac = file.getParentFile().getName();

		String dateStrs[] = pattern1.split(fileName);

		String yearstr = dateStrs[dateStrs.length-1];
		String Tm[] = pattern2.split(yearstr);
		/**
		 * 2015年12月18日 15:56:54 修改时间
		 * 
		 */
		String testtime = Tm[0].substring(0, 6);
		// Tm[0]+Tm[1]; //该时间为从文件名中截取出来的日期

		String testtimes = UtilDate.getMonth();
		// System.out.println(testtimes+"   :::::");
		/**
		 * 2016年1月6日 11:54:05修改
		 * 
		 * 匹配文件生成时间问题
		 */
		if (!testtime.equals(testtimes)) {
			return;
		}

		
			// 获取appid
				String appid = "";
				String appidStr = filePath.replace(ConfParser.org_prefix, "");
				while (appidStr.startsWith(File.separator)) {
					appidStr = appidStr.substring(1, appidStr.length());
				}
				appidStr = appidStr.substring(0, appidStr.indexOf(File.separator));
				if (this.appid.contains(appidStr)) {
					appid = appidStr;
				} else {
					appid = "";
				}
		
		
		
		String fileIndex = mac + "|" + file.getName() + "_" + 0;
		allMap = putMapData(headerMap, numType, allMap, file, fileIndex);
		allMap.put("fileName", fileName);
		allMap.put("filePath", filePath);
		allMap.put("fileSize", fileSize);
		allMap.put("file_lastModifies", file_lastModifies);
		allMap.put("app_id", appid);
		flag = insertMysqlDaoByPc.insertMysqlDB(allMap, numType, org, file, testtime);
		// 备份数据
		System.out.println("结束插入数据  开始备份文件************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		for (int i = 0; i < filepaths.length; i++) {
			String newfilePath = filepaths[i];
			if (newfilePath == null || newfilePath.isEmpty()) {
				continue;
			}
			File destFile = new File(newfilePath.replace(ConfParser.org_prefix, destFilePath + File.separator));
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
				} finally {
					newFile.delete();
				}
			}
		}
		System.out.println("结束备份************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		System.out.println("更新数据：" + flag);
	}

	/**
	 * 遍历已取出的map数据，bodymap数据
	 * 
	 * @param DataMap
	 * @param numType
	 * @param allMap
	 * @param file
	 * @param fileIndex
	 * @return
	 */
	private Map putMapData(Map DataMap, String numType, Map allMap, File file, String fileIndex) {
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
		allMap.put("file_index", fileIndex);
		return allMap;
	}

	/**
	 * 对字符串的截取
	 * 
	 * @param str
	 * @return
	 * @return String
	 */
	public String subTxt(String str) {
		if (str.startsWith("{")) {
			str = str.substring(1, str.length());
		}
		if (str.endsWith("}")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}

	/**
	 * 对省市县信息进行处理
	 * 
	 * @param str
	 * @return
	 * @return String
	 */
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
					System.out.println(str + "--->请求百度失败");
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

			if (snStart > 0) {
				String snStr = str.substring(snStart);
				if (snStr != null && snStr.length() > 0) {
					int snEnd = snStart + 16;
					String snStr1 = str.substring(snEnd);
					System.out.println(snStr1);
					int snIndex_end = snStr1.indexOf("\"");
					String street_number = snStr1.substring(0, snIndex_end);
					if (StringUtils.isNotBlank(street_number)) {
						ssq += street_number + "_";
					} else {
						ssq += "-_";
					}
				}
			} else {
				ssq += "-_";
			}
			if (ssq.endsWith("_")) {
				ssq = ssq.substring(0, ssq.length() - 1);
			}
			return ssq;
		}
		return null;
	}

	/**
	 * GPS坐标转换
	 * 
	 * @param gps
	 * @return
	 */
	private static String getGpsPoint(String gps) {
		String gpsPoint = "";
		if (gps.contains("°")) {
			String degrees = gps.substring(0, gps.lastIndexOf("°"));
			String minutes = gps.substring(gps.lastIndexOf("°") + 1, gps.lastIndexOf("′"));
			String seconds = gps.substring(gps.lastIndexOf("′") + 1, gps.lastIndexOf("″"));

			// Long gpsLong =
			// Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
			float gpsLong = Float.parseFloat(degrees) + Float.parseFloat(minutes) / 60 + Float.parseFloat(seconds) / 3600;

			DecimalFormat decimalFormat = new DecimalFormat(".0000000");
			gpsPoint = decimalFormat.format(gpsLong);
		} else {
			if (gps.contains("E")) {
				gpsPoint = gps.substring(0, gps.lastIndexOf("E"));
				float gpsLong = Float.valueOf(gpsPoint);
				DecimalFormat decimalFormat = new DecimalFormat(".0000000");
				gpsPoint = decimalFormat.format(gpsLong);
			}
			if (gps.contains("N")) {
				gpsPoint = gps.substring(0, gps.lastIndexOf("N"));
				float gpsLong = Float.valueOf(gpsPoint);
				DecimalFormat decimalFormat = new DecimalFormat(".0000000");
				gpsPoint = decimalFormat.format(gpsLong);
			}
		}
		return gpsPoint;
	}

	/**
	 * GPS坐标转换
	 * 
	 * @param meta
	 * @return
	 */
	private static String[] transGpsPoint(String meta) {

		String[] result = new String[2];
		String[] info = null;
		if (meta != null && !"".equals(meta)) {
			info = meta.split(" ");
		} else {
			return null;
		}

		String latitude = "";
		String longitude = "";

		String gpsPoint = "";
		try {
			for (int i = 0; i < info.length && i < 2; i++) {
				if (info[i].contains("°")) {
					String degrees = info[i].substring(0, info[i].lastIndexOf("°"));
					String minutes = info[i].substring(info[i].lastIndexOf("°") + 1, info[i].lastIndexOf("′"));
					String seconds = info[i].substring(info[i].lastIndexOf("′") + 1, info[i].lastIndexOf("″"));

					// Long gpsLong =
					// Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
					float gpsLong = Float.parseFloat(degrees) + Float.parseFloat(minutes) / 60 + Float.parseFloat(seconds) / 3600;

					DecimalFormat decimalFormat = new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if (info[i].contains("E")) {
						longitude = gpsPoint;
					} else if (info[i].contains("N")) {
						latitude = gpsPoint;
					} else if (gpsLong > 80.0) {
						longitude = gpsPoint;
					} else {
						latitude = gpsPoint;
					}
				} else {
					if (info[i].contains("E")) {
						gpsPoint = info[i].substring(0, info[i].lastIndexOf("E"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						longitude = gpsPoint;
					} else if (info[i].contains("N")) {
						gpsPoint = info[i].substring(0, info[i].lastIndexOf("N"));
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						latitude = gpsPoint;
					} else {
						gpsPoint = info[i];
						float gpsLong = Float.valueOf(gpsPoint);
						DecimalFormat decimalFormat = new DecimalFormat(".0000000");
						gpsPoint = decimalFormat.format(gpsLong);
						if (gpsLong > 80.0) {
							longitude = gpsPoint;
						} else {
							latitude = gpsPoint;
						}
					}
				}
			}
			result[0] = longitude;
			result[1] = latitude;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	// -------------------------------------------------------------------------------------------------------------
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

	public SpecialPcTestData() {
		super();
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	public SpecialPcTestData(String root) {
		super();
		this.root = root;
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}
}
