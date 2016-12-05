package com.opencassandra.resultsettest.separatebypc.submeter_pc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import cn.speed.jdbc.utils.JDBCUtils;

import com.csvreader.CsvReader;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.resultsettest.separateapp.GetPostTest;

public class ResultsetTestData_Pc {
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
	private static String ak = ConfParser.ak;
	int counts = 0;
	int errors = 0;
	public static int count = 0;
	private static ResultsetTestDao_Pc resultsetTestDao_Pc = new ResultsetTestDao_Pc();
	private List appid = new ArrayList();
	private static String pathType = ConfParser.pathType;
	static String basename = "";
	//存放已解析过的文件路径
	private List<String> filepathList = new ArrayList<String>();
	private static List datas = new ArrayList();
	public static int getCount() {
		return count;
	}

	synchronized public static void setCount() {
		ResultsetTestData_Pc.count++;
	}

	public static int dataIndex = 256146;
	public static int successCount = 0;
	public static int failCount = 0;
	static Statement statement;
	static Connection conn;
	StringBuffer sql = new StringBuffer();
	private ExecutorService analyzeThreadPool;

	static String destPathPattern = "ORGKEY/SERVICETYPE/TERMINALTYPE/TERMINALMODEL/IMEI";

	static String regexName = "[^_.]+";
	// static String regexName = "[^_.-]+";
	static String regexPath = "[^/\\\\]+";

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

	static public Map<String, String> fileNameSpilt(String fileNamePattern, String fileName, String regexName) {
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

	public ResultsetTestData_Pc() {
		super();
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	public ResultsetTestData_Pc(String root) {
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

	//	String [] args ={"201606061600"} ;
		
		String testtime = "";
		if (args.length > 0) {
			 testtime = args[0];
			/*Date dBefore = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
			//SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd00");
			Date date;
			try {
				date = sdf.parse(newTimes);
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date);
				calendar.add(Calendar.HOUR, -1); // 设置为前一个小时
				dBefore = calendar.getTime(); // 得到前一个小时的时间
				String testtime2 = sdf.format(dBefore);
				testtime = newTimes + "," + testtime2;
				System.out.println("testtime   :::::::   " + testtime);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}else{
			testtime = ConfParser.time;
		}

		
		ResultsetTestData_Pc desc = new ResultsetTestData_Pc();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> Start!");
		desc.setChange(true);
		String file[] = new String[] {};
		desc.appid = desc.getAppids();// 获取匹配的文件的下级目录名
		System.out.println(desc.appid);
		file = ConfParser.srcReportPath;// 获取要解析文件的根目录 就是appid文件的上级目录
		
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
				desc.findFile(filePath, org,testtime);
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
				desc.findFile(filePath, org,testtime);
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
					desc.findFile(filePath, org,testtime);
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
							desc.findFile(rootFile.getAbsolutePath(), rootFileOrg,testtime);
						}
					} else {
						
						
						/**
						 * 2016年6月16日11:13:13 修改 添加是否分表开关 若issubmeter=no
						 * 则不分表，其他情况都视为分表情况
						 */
						if (ConfParser.issubmeter.equals("no")) {
							basename = dataname;
						} else {
							basename = resultsetTestDao_Pc.getBaseName(code2);

							if (basename.isEmpty()) {
								basename = dataname;
							}

						}
						resultsetTestDao_Pc.startOrCreateDB(basename);
						
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
						String[] paths = getPostTest.getPaths(desc.appid, keyList,testtime);
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
							desc.findFile(filePath, org,testtime);
						}
					}

				}
			}
		} else {
			/**
			 * 不带Default的解析路径获取
			 */
			
			
			/**
			 * 2015年12月9日 15:02:06 修改 添加有关数据分库分表程序中的根据code获取到相应的数据库名
			 * 并根据数据库名创建相应的数据库
			 * 
			 * 2015年12月11日 16:01:39修改 添加是否分表开关 若issubmeter=no
			 * 则不分表，其他情况都视为分表情况
			 */
			/*if (ConfParser.issubmeter.equals("no")) {
				basename = dataname;
			} else {
				basename = resultsetTestDao_Pc.getBaseName(code1);

				if (basename.isEmpty()) {
					basename = dataname;
				}

			}
			resultsetTestDao_Pc.startOrCreateDB(basename);*/

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
					desc.findFile(filePath, org,testtime);
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
					desc.findFile(filePath, org,testtime);
				}
			}
		}
		if (datas.size() > 0 && datas != null) {
			boolean judge = true;
			try {
				System.out.println("共解析文件：" + datas.size() + "个");
				String url = ConfParser.dsturl.substring(0,
						ConfParser.dsturl.lastIndexOf("/") + 1)
						+ ResultsetTestData_Pc.basename;
				start(url, ConfParser.dstuser, ConfParser.dstpassword);
				for (Object o : datas) {
					judge = true;
					//存放文件与(sql、表名)
					List data = (List) o;
					//存放sql与表名
					List sqls = (List) data.get(1);
					String sql = "";
					for (Object o2 : sqls) {
						List tabANDsql = (List) o2;
						try {
							sql = (String) tabANDsql.get(2);
							System.out.println("sql语句：\t" + sql);
							statement.execute(sql);
						} catch (Exception e) {
							String table = tabANDsql.get(0).toString();
							boolean flage = ResultsetTestDao_Pc
									.queryTabExist(table);
							if (!flage) {
								ResultsetTestDao_Pc.createTable(table);
								try {
									statement.execute(sql);
								} catch (Exception e2) {
									judge = false;
								}
								boolean opFla = ResultsetTestDao_Pc
										.queryTabExist("date_tablename");
								if (!opFla) {
									ResultsetTestDao_Pc
											.createDateTab("date_tablename");
								}
								boolean queryF = ResultsetTestDao_Pc
										.queryDataExist(table, table);
								if (!queryF) {
									try {
										statement.execute(tabANDsql.get(1).toString());
									} catch (Exception e2) {
										e2.printStackTrace();
									}
								}
							} else {
								judge = false;
							}
						}
					}
					BackupFile((File) data.get(0), judge);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					close();
				} catch (SQLException e2) {
					e2.printStackTrace();
				}
			}
		}
		// desc.insertMysqlDaoByPc.execute("pro_detail_data_bak");
		System.out.println("counts: " + desc.counts);
		System.out.println("successCount: " + successCount);
		System.out.println("failCount: " + failCount);
		desc.getAnalyzeThreadPool().shutdown();
		dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> End!");
		map.put("存储路径", mapCount++);
		// queryCassandraReport();
		// queryCassandraReportTest();

	}

	/**
	 * 根据文件配置appname获取查询出的appid用以确定文件下级目录名
	 * 
	 * @return appid组成的list appids
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
			appids = resultsetTestDao_Pc.getappIds(sql);
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
	 */
	public void findFile(String rootDirectory, String org,String testtime) {
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
		String dateString = dateFormat.format(new Date());
		// long nums = list.length;
		StringBuffer errStr = new StringBuffer();
		try {
			for (int i = 0; i < fileList.length; i++) {
				File myFile = fileList[i];
				if (myFile.isDirectory()) {
					length = length + myFile.listFiles().length;
					findFile(myFile.getAbsolutePath(), org,testtime);
				}else{
					boolean judge = true;
					for (String filePath : filepathList) {
						if (filePath.equals(myFile.getAbsolutePath())) {
							judge = false;
							return;
						} else {
							judge = true;
						}
					}
					if (judge) {
						filepathList.add(myFile.getAbsolutePath());
						try {
							// File filePath = new
							// File(myFile.getAbsolutePath());
							try {
								// 判断文件格式并解析
								this.deal(myFile, 01001, org, testtime);
							} catch (Exception e) {
								e.printStackTrace();
								errStr.append(myFile.getAbsolutePath()
										+ ":\r\n");
								errStr.append(e.toString() + "\r\n");
								errors++;
								System.out.println("文件处理异常>>>"
										+ Paths.get(myFile.getAbsolutePath()));
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
				}
			}
//			Iterator iter = timeMap.keySet().iterator();
			// System.out.println(dateString + ">>> 发现待处理文件数量为： " + length);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}

	// 85.228:9160
	private ArrayList<String> getFolderList(String rootDirectory, ArrayList<String> folderList) {
		File file = new File(rootDirectory);
		File[] list = file.listFiles();
		for (int i = 0; i < list.length; i++) {
			File sonFile = list[i];
			if (sonFile.isDirectory()) {
				folderList.add(sonFile.getAbsolutePath());
				// folderList = getFolderList(sonFile.getAbsolutePath(),
				// folderList);
			}
		}
		return folderList;
	}

	public void findFile() {
		// findFile(this.root);
	}

	/**
	 * 判断文件格式，并解析文件
	 * 
	 * @param file
	 *            要判断的文件
	 * @param ServiceId
	 * @param org
	 */
	public void deal(File file, int ServiceId, String org,String testtime) {
		if (file.getAbsoluteFile().toString().endsWith(".abstract.csv") || file.getAbsoluteFile().toString().endsWith(".summary.csv")) {

			// 解析文件
			splitAbstractCsv(file, 01001, org,testtime);
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
	public boolean splitAbstractCsv(File filePath, int ServiceId, String org,String testtime) {
		StringBuffer errStr1 = new StringBuffer("");
		try {
			ArrayList<String[]> csvHeader = new ArrayList<String[]>();// 文头
			ArrayList<String[]> csvBody = new ArrayList<String[]>();// 文体

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
						} else if (dataEreaIndex == 1) {
							csvBody.add(csvList.get(i));
							// } else if (dataEreaIndex == 2) {
							// testScenario.add(csvList.get(i));
						}
					} else {

					}
				}
			}
			this.writeToFile(csvHeader, csvBody, filePath, org,testtime);
		} catch (Exception e) {
			copyErrFile(filePath);
			errStr1.append(filePath.getAbsolutePath() + ":\r\n");
			errStr1.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		return true;
	}

	private void writeToFile(ArrayList<String[]> absHeader, ArrayList<String[]> absBody, File filePath, String org,String testtime) {
		StringBuffer errStr2 = new StringBuffer("");
		// 获取测试类型Num
		Pattern pattern1 = Pattern.compile("[-_.]");
		Pattern pattern2 = Pattern.compile("\\d+");
		String numStrs[] = pattern1.split(filePath.getName());
		String urladd[] = null;
		String datatime[] = null;
		if (absBody == null || absBody.size() == 0) {
			datatime = new String[1];
			urladd = new String[1];
		} else {
			datatime = new String[absBody.size()];
			urladd = new String[absBody.size()-1];
		}
		// 要生成的TXT的文件内容
		StringBuffer str = new StringBuffer();
		// 头部Map
		Map absHeaderMap = new HashMap();
		// 主体Map
		Map absBodyMap = new HashMap();
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

			if (absBody != null && absBody.size() > 0) {
				String[] key = absBody.get(0);// 主体的表头
				if (key != null) {
					for (int i = 1; i < absBody.size(); i++) {// 主体的数据
						try { // 第i排数据，i从1开始
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
									if("目标地址".equals(sonKey) || "下载地址".equals(sonKey) || "上传地址".equals(sonKey)){
										urladd[i-1] = sonValue;
									}
									map.put(sonKey, sonValue);
								}
								if (key[0].contains("测试时间") || key[0].equals("测试开始时间") || key[0].trim().toLowerCase().equals("time") || key[0].trim().toLowerCase().equals("start time")
										|| key[0].trim().toLowerCase().equals("test start time") || key[0].trim().toLowerCase().equals("network 1time")) {
									datatime[i - 1] = value[0];
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

					body = subTxt(body);
					header = subTxt(header);

					str.append(body);
					if (!body.trim().equals("") && !header.trim().equals("")) {
						str.append(",");
					}
					str.append(header);
					if (!header.trim().equals("")) {
						str.append(",");
					}
				} else if (absBodyMap.size() > 1) {
					for (int i = 0; i < absBodyMap.size(); i++) {
						Map nextMap = (Map) absBodyMap.get(i + 1);
						String body = nextMap.toString();
						String header = absHeaderMap.toString();

						body = subTxt(body);
						header = subTxt(header);

						str.append(body);
						if (!body.trim().equals("") && !header.trim().equals("")) {
							str.append(",");
						}
						str.append(header);
						if (!header.trim().equals("")) {
							str.append(",");
						}
						str.append("\r\n");
					}
				} else {
					String header = absHeaderMap.toString();
					header = subTxt(header);

					str.append(header);
					if (!header.trim().equals("")) {
						str.append(",");
					}
				}
			}

		} catch (Exception e) {
			copyErrFile(filePath);
			errStr2.append(filePath.getAbsolutePath() + ":\r\n");
			errStr2.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		try {
			setMapData(absHeaderMap, absBodyMap, numStrs[1], filePath.getAbsoluteFile(), org,testtime,urladd);
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
	 * 
	 * @param headerMap
	 * @param bodyMap
	 * @param numType
	 * @param file
	 * @param org
	 */
	private void setMapData(Map headerMap, Map bodyMap, String numType, File file, String org,String testtimes,String[] urladd) {
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
//		String destFilePath = ConfParser.backReportPath;
//		// 要解析的文件名 00000000_04002.000_0-2015_08_18_16_21_46_655.summary.csv
//		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
//		// 截取文件的标识格式summary
//		String filenameorg = path1.substring(path1.lastIndexOf("."), path1.length());
//		// 文件标识格式
//		String[] files = new String[] { filenameorg, ".detail", ".deviceInfo", ".monitor" };
//		// 文件备份的标识格式
//		String[] filepaths = new String[4];
//		for (int i = 0; i < files.length; i++) {
//			String newfilePath = file.getAbsolutePath().replace(filenameorg, files[i]);
//			filepaths[i] = newfilePath;
//		}
		List<List<String>> sqlList = new ArrayList<List<String>>();
		List data = new ArrayList();
		String test_time = testtimes.substring(0,6);
		
		String mac = file.getParentFile().getName();

		if (bodyMap != null) {
			if (bodyMap.size() == 1) {
				String fileIndex = mac + "|" + file.getName() + "_" + 0;
				Map bodyNextMap = (Map) bodyMap.get(1);
				// 遍历map数据并把数据重新放入allmap中
				allMap = putMapData(headerMap, numType, allMap, file, fileIndex, urladd[0]);
				allMap = putMapData(bodyNextMap, numType, allMap, file, fileIndex, urladd[0]);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				sqlList.add(resultsetTestDao_Pc.insertMysqlDB(allMap, numType, org, file,test_time));
			} else if (bodyMap.size() > 1) {
				for (int i = 0; i < bodyMap.size(); i++) {
					String fileIndex = mac + "|" + file.getName() + "_" + i;
					Map bodyNextMap = (Map) bodyMap.get(i + 1);
					allMap = putMapData(headerMap, numType, allMap, file, fileIndex, urladd[i]);
					allMap = putMapData(bodyNextMap, numType, allMap, file, fileIndex, urladd[i]);
					allMap.put("fileName", fileName);
					allMap.put("filePath", filePath);
					allMap.put("fileSize", fileSize);
					allMap.put("file_lastModifies", file_lastModifies);
					sqlList.add(resultsetTestDao_Pc.insertMysqlDB(allMap, numType, org, file,test_time));
				}
			} else {
				String fileIndex = mac + "|" + file.getName() + "_" + 0;
				allMap = putMapData(headerMap, numType, allMap, file, fileIndex, urladd[0]);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				sqlList.add(resultsetTestDao_Pc.insertMysqlDB(allMap, numType, org, file,test_time));
			}
		} else {
			String fileIndex = mac + "|" + file.getName() + "_" + 0;
			allMap = putMapData(headerMap, numType, allMap, file, fileIndex, urladd[0]);
			allMap.put("fileName", fileName);
			allMap.put("filePath", filePath);
			allMap.put("fileSize", fileSize);
			allMap.put("file_lastModifies", file_lastModifies);
			sqlList.add(resultsetTestDao_Pc.insertMysqlDB(allMap, numType, org, file,test_time));
		}
		data.add(file);
		data.add(sqlList);
		datas.add(data);
//		// 备份数据
//		System.out.println("结束插入数据  开始备份文件************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
//		for (int i = 0; i < filepaths.length; i++) {
//			String newfilePath = filepaths[i];
//			if (newfilePath == null || newfilePath.isEmpty()) {
//				continue;
//			}
//			File destFile = new File(newfilePath.replace(ConfParser.org_prefix, destFilePath + File.separator));
//			File newFile = new File(newfilePath);
//			if (!newFile.exists()) {
//				continue;
//			}
//			if (destFile.exists()) {
//				destFile.delete();
//			}
//			if (!destFile.exists()) {
//				if (!destFile.getParentFile().exists()) {
//					destFile.getParentFile().mkdirs();
//				}
//				try {
//					destFile.createNewFile();
//					FileUtils.copyFile(newFile, destFile);
//				} catch (IOException e) {
//					e.printStackTrace();
//				} finally {
//					newFile.delete();
//				}
//			}
//		}
//		System.out.println("结束备份************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
//		System.out.println("更新数据：" + flag);
	}
	/**
	 * 备份文件
	 * @param file
	 */
	public static void BackupFile(File file, boolean falg) {
		// 解析文件的绝对路径
		String filePath = file.getAbsolutePath().replace(
				ConfParser.org_prefix.replace(File.separator, "|"), "");
		String destFilePath = "";
		if (falg) {
			destFilePath = ConfParser.backReportPath;
		} else {
			destFilePath = ConfParser.bug_reportpath;
		}

		// 要解析的文件名 00000000_04002.000_0-2015_08_18_16_21_46_655.summary.csv
		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
		// 截取文件的标识格式summary
		String filenameorg = path1.substring(path1.lastIndexOf("."),
				path1.length());
		// 文件标识格式
		String[] files = new String[] { filenameorg, ".detail", ".deviceInfo",
				".monitor" };
		// 文件备份的标识格式
		String[] filepaths = new String[4];
		for (int i = 0; i < files.length; i++) {
			String newfilePath = file.getAbsolutePath().replace(filenameorg,
					files[i]);
			filepaths[i] = newfilePath;
		}
		System.out
				.println("结束插入数据  开始备份文件************："
						+ new Date().toLocaleString() + "   毫秒："
						+ new Date().getTime());
		for (int i = 0; i < filepaths.length; i++) {
			String newfilePath = filepaths[i];
			if (newfilePath == null || newfilePath.isEmpty()) {
				continue;
			}
			File destFile = new File(newfilePath.replace(ConfParser.org_prefix,
					destFilePath + File.separator));
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
		System.out.println("结束备份************：" + new Date().toLocaleString()
				+ "   毫秒：" + new Date().getTime());
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
	private Map putMapData(Map DataMap, String numType, Map allMap, File file, String fileIndex,String urladd) {
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
		allMap.put("url", urladd);
		allMap.put("file_index", fileIndex);
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
		String sql = "select * from " + tableName + " where id=" + id + " and gpsInfoX!='N/A'";
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
		String sql = "select * from " + tableName + " where store_path like '%" + filePath + "%'";
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

		URL url = new URL("http://api.map.baidu.com/geocoder?ak=" + ak + "&coordtype=wgs84ll&location=" + x + "," + y + "&output=json");
		URLConnection connection = url.openConnection();
		/**
		 * 然后把连接设为输出模式。URLConnection通常作为输入来使用，比如下载一个Web页。
		 * 通过把URLConnection设为输出，你可以把数据向你个Web页传送。下面是如何做：
		 */
		System.out.println(url);
		connection.setDoOutput(true);
		OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(), "utf-8");
		// remember to clean up
		out.flush();
		out.close();
		// 一旦发送成功，用以下方法就可以得到服务器的回应：
		String res;
		InputStream l_urlStream;
		l_urlStream = connection.getInputStream();
		BufferedReader in = new BufferedReader(new InputStreamReader(l_urlStream, "UTF-8"));
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

	/**
	 * 建立数据库连接
	 * 
	 * @param url
	 * @param user
	 * @param password
	 * @return void
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static void start(String url, String user, String password)
			throws Exception {
		System.out.println(url + " :数据库连接: " + user + " :: " + password);
		close();
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
	public static void close() throws SQLException {
		// TODO Auto-generated method stub
		if (statement != null) {
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
}
