package com.opencassandra.resultsettest;

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
import java.sql.SQLException;
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
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.csvreader.CsvReader;
import com.opencassandra.descfile.ConfParser;
import com.opencassandra.descfile.GetPostTest;
import com.opencassandra.service.utils.UtilDate;

public class ResultsetTestData {
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
	private static String pathType = ConfParser.pathType;
	int counts = 0;
	int errors = 0;
	public static int count = 0;
	private static ResultsetTestDao resultsetDao = new ResultsetTestDao();
	private List appid = new ArrayList();
	private List sqlList = new ArrayList();
	static String basename = "";

	// 截取报告入库时间的rex
	private static Pattern pattern1 = Pattern.compile("[-]");
	private static Pattern pattern2 = Pattern.compile("[_]");

	public static int getCount() {
		return count;
	}

	synchronized public static void setCount() {
		ResultsetTestData.count++;
	}

	public static int successCount = 0;
	public static int failCount = 0;
	private ExecutorService analyzeThreadPool;

	/**
	 * @override
	 */
	public void run() {
		System.out.println("counts: " + this.getCounts());
	}

	static public void main(String[] args) {
		ResultsetTestData desc = new ResultsetTestData();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> Start!");
		desc.setChange(true);
		desc.appid = desc.getAppids();
		String file[] = new String[] {};
		file = ConfParser.srcReportPath;
		System.out.println("开始获取路径进行遍历" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		String code1 = (ConfParser.code != null && ConfParser.code.length > 0) ? ConfParser.code[0] : "";
		String dataname = ConfParser.subdatabase;
		if ("all".equals(code1)) {
		} else {
			if (pathType.equals("default")) {
				for (int i = 0; i < file.length; i++) {
					String filePath = file[i];
					System.out.println(filePath);
					if (filePath == null || filePath.isEmpty()) {
						continue;
					}
					String org = "";
					String filePathOrg = "";
					if (filePath.endsWith("Default")) {
						filePathOrg = filePath.replace(File.separator + "Default", "");
					}else{
						filePathOrg = filePath;
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
					if (filePath == null || filePath.isEmpty()) {
						continue;
					}
					String org = "";
					while (filePath.endsWith(File.separator)) {
						filePath = filePath.substring(0, filePath.length() - 1);
					}
					org = filePath.substring(filePath.lastIndexOf(File.separator) + 1, filePath.length());
					desc.findFile(filePath, org);
				}
			}
		}
		GetPostTest getPostTest = new GetPostTest();
		String codeUrl = ConfParser.getCodePath;
		if (pathType.equals("default")) {
			/**
			 * 层级结构的路径 ： 需调用带有Default逻辑的方法
			 */
			if (codeUrl.isEmpty()) {
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
						filePathOrg = filePath.replace(File.separator + "Default", "");
					}else{
						filePathOrg=filePath;
					}
					if (filePathOrg.contains(File.separator)) {
						org = filePathOrg.substring(filePathOrg.lastIndexOf(File.separator) + 1, filePathOrg.length());
					}
					desc.findFile(filePath, org);
				}
			} else {
				for (int j = 0; j < ConfParser.code.length; j++) {
					ArrayList keyList = new ArrayList();
					String code2 = ConfParser.code[j];
					if (code2 == null || code2.isEmpty()) {
						continue;
					}
					String codeStr = "code=" + code2;
					//System.out.println(ConfParser.url);

					/**
					 * 2015年12月11日 16:01:39修改 添加是否分表开关 若issubmeter=no
					 * 则不分表，其他情况都视为分表情况
					 */
					if (ConfParser.issubmeter.equals("no")) {
						basename = dataname;
					} else {
						basename = resultsetDao.getBaseName(code2);

						if (basename.isEmpty()) {
							basename = dataname;
						}

					}
					resultsetDao.startOrCreateDB(basename);

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
							System.out.println(rootFileOrg + "  ::::::");
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
							}else{
								filePathOrg=filePath;
							}
							if (filePathOrg.contains(File.separator)) {
								org = filePathOrg.substring(filePathOrg.lastIndexOf(File.separator) + 1, filePathOrg.length());
							}
							// org =
							// filePath.substring(filePath.lastIndexOf(File.separator)+1,
							// filePath.length());
							
							System.out.println(org+"     ::::::::");
							
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
				if (code1.equals("all")) {
					/**
					 * 2015年12月9日 15:02:06 修改 添加有关数据分库分表程序中的根据code获取到相应的数据库名
					 * 并根据数据库名创建相应的数据库
					 * 
					 * 2015年12月11日 16:01:39修改 添加是否分表开关 若issubmeter=no
					 * 则不分表，其他情况都视为分表情况
					 */
					if (ConfParser.issubmeter.equals("no")) {
						basename = dataname;
					} else {
						basename = resultsetDao.getBaseName(code1);

						if (basename.isEmpty()) {
							basename = dataname;
						}

					}
					resultsetDao.startOrCreateDB(basename);


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
		}
		if (desc.sqlList.size() > 1) {
			desc.dealData();
			desc.sqlList.clear();
		}
		// desc.insertMysqlDao.execute("pro_deal_data");
		System.out.println("counts: " + desc.counts);
		System.out.println("successCount: " + successCount);
		System.out.println("failCount: " + failCount);
		desc.getAnalyzeThreadPool().shutdown();
		dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> End!");
		map.put("存储路径", mapCount++);
	}

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
			appids = resultsetDao.getappIds(sql);
			if (flag) {
				appids.add("default_app_id");
			}
			return appids;
		}
	}

	public void findFile(String rootDirectory, String org) {
		System.out.println("开始findFile************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		File rootFile = new File(rootDirectory);
		

		System.out.println(org+"     ::::::::findFile");
		
		if (!rootFile.exists()) {
			// rootFile.mkdir();
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
				System.out.println(myFile.getAbsolutePath() + "  ::::::");
				if (myFile.isDirectory()) {
					length = length + myFile.listFiles().length;
					findFile(myFile.getAbsolutePath(), org);
				}
				try {
					try {
						this.deal(myFile, 01001, org);

					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						errStr.append(myFile.getAbsolutePath() + ":\r\n");
						errStr.append(e.toString() + "\r\n");
						errors++;
						System.out.println("文件处理异常>>>" + Paths.get(myFile.getAbsolutePath()));
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

	/**
	 * 
	 * @param file
	 * @param ServiceId
	 * @param org
	 * @return void
	 */
	public void deal(File file, int ServiceId, String org) {
		System.out.println(file.getAbsolutePath() + "   ::::::::::");
		
		System.out.println(org+"     ::::::::deal");
		if (file.getAbsoluteFile().toString().endsWith(".abstract.csv") || file.getAbsoluteFile().toString().endsWith(".summary.csv")) {
			System.out.println("开始deal************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
			splitAbstractCsv(file, 01001, org);
			setCount();
		} else {
			return;
		}
	}

	/**
	 * 进行报告分类，并根据不同标识把报告数据进行不同的处理
	 * 
	 * @param filePath
	 * @param ServiceId
	 * @param org
	 * @return
	 * @return boolean
	 */
	public boolean splitAbstractCsv(File filePath, int ServiceId, String org) {
		System.out.println("开始splitAbstractCsv************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
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
							if (csvList.get(i)[0].equals("测试时间") || csvList.get(i)[0].equals("测速类型")) {
								dataEreaIndex = 1;
							} else if (csvList.get(i)[0].equals("--- Test Scenario ---")) {
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
				if (csvList == null || csvList.size() == 0) {
					return false;
				}
				for (int i = 0; i < csvList.size(); i++) {
					if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
						if (csvList.get(i)[0] != null) {
							if ((csvList.get(i)[0].equals("测试时间") || csvList.get(i)[0].trim().toLowerCase().equals("time") || csvList.get(i)[0].trim().toLowerCase().equals("test time"))
									&& csvList.size() > i + 1
									&& csvList.get(i + 1) != null
									&& csvList.get(i + 1).length >= 2
									&& csvList.get(i + 1)[0] != null
									&& (csvList.get(i + 1)[0].contains("型号") || (csvList.get(i + 1)[0].trim().toLowerCase().contains("device type")
											|| csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal type") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal mode")
											|| csvList.get(i + 1)[0].trim().toLowerCase().contains("device model") || csvList.get(i + 1)[0].trim().toLowerCase().contains("terminal model")))) {

							} else if ((csvList.get(i)[0].equals("测试时间") || csvList.get(i)[0].equals("测试开始时间") || csvList.get(i)[0].equals("时间")
									|| (csvList.get(i)[0].equals("测速类型") || csvList.get(i)[0].equals("测试类型")) || csvList.get(i)[0].equals("网络制式1时间") || csvList.get(i)[0].equals("网络(1)时间")
									|| csvList.get(i)[0].equals("Network 1Time") || csvList.get(i)[0].equals("Network(1) Time") || csvList.get(i)[0].trim().toLowerCase().equals("time")
									|| csvList.get(i)[0].trim().toLowerCase().equals("duration") || csvList.get(i)[0].trim().toLowerCase().equals("start time")
									|| csvList.get(i)[0].trim().toLowerCase().equals("test start time") || (csvList.get(i)[0].trim().toLowerCase().equals("time") && (csvList.get(i)[1] != null
									&& csvList.get(i)[1].trim().toLowerCase().equals("network") || csvList.get(i)[1].trim().toLowerCase().equals("website") || csvList.get(i)[1].trim().toLowerCase()
									.equals("address"))))
									&& csvList.get(i).length > 2 && csvList.get(i)[2] != null && !csvList.get(i)[2].equals("")) {
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
			System.out.println(org+"     ::::::::split");
			this.writeToFile(absHeader, absBody, testScenario, filePath, org);
		} catch (Exception e) {
			System.out.println("filePath:  " + filePath.getAbsolutePath());
			errStr1.append(filePath.getAbsolutePath() + ":\r\n");
			errStr1.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 处理报告
	 * 
	 * @param absHeader
	 * @param absBody
	 * @param testScenario
	 * @param filePath
	 * @param org
	 * @return void
	 */
	private void writeToFile(ArrayList<String[]> absHeader, ArrayList<String[]> absBody, ArrayList<String[]> testScenario, File filePath, String org) {
		System.out.println("开始writeToFile************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		StringBuffer errStr2 = new StringBuffer("");
		
		
		System.out.println(org+"     ::::::::writeToFile");
		// 获取测试类型Num
		Pattern pattern1 = Pattern.compile("[-_.]");
		Pattern pattern2 = Pattern.compile("\\d+");
		String numStrs[] = pattern1.split(filePath.getName());
		String datatime[] = null;
		if (absBody == null || absBody.size() == 0) {
			datatime = new String[1];
		} else {
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
					if (absHeader.get(i).length >= 2) {
						value = absHeader.get(i)[1];
					}
					if ((key.contains("测试时间") || key.equals("测试开始时间") || key.equals("时间") || key.trim().toLowerCase().contains("time") || key.trim().toLowerCase().equals("start time") || key.trim()
							.toLowerCase().equals("test start time"))) {
						datatime[0] = value;
					}
					absHeaderMap.put(key, value);
				} catch (ArrayIndexOutOfBoundsException e) {
					absHeaderMap.put(key, "");
					System.out.println("filePath:  " + filePath.getAbsolutePath());
					errStr2.append(filePath.getAbsolutePath() + ":\r\n");
					errStr2.append(e.toString() + "\r\n");
					e.printStackTrace();
				} catch (Exception e) {
					absHeaderMap.put(key, "");
					System.out.println("filePath:  " + filePath.getAbsolutePath());
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
							if (testScenario.get(i).length >= 2) {
								value = testScenario.get(i)[1];
							}
						}
						if (key.equals("") && testScenario.get(i + 1).length >= 2 && testScenario.get(i + 1)[0] != null) {
							testScenarioMap.put(key, value);
						} else if (!key.equals("")) {
							testScenarioMap.put(key, value);
						}
					}

				} catch (ArrayIndexOutOfBoundsException e) {
					testScenarioMap.put(key, "");
					System.out.println("filePath:  " + filePath.getAbsolutePath());
					errStr2.append(filePath.getAbsolutePath() + ":\r\n");
					errStr2.append(e.toString() + "\r\n");
					e.printStackTrace();
				} catch (Exception e) {
					testScenarioMap.put(key, "");
					System.out.println("filePath:  " + filePath.getAbsolutePath());
					errStr2.append(filePath.getAbsolutePath() + ":\r\n");
					errStr2.append(e.toString() + "\r\n");
					e.printStackTrace();
				}
			}
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
									map.put(sonKey, sonValue);
								}
								if ((key[0].contains("测试时间") || key[0].equals("测试开始时间") || key[0].equals("时间") || key[0].trim().toLowerCase().equals("time")
										|| key[0].trim().toLowerCase().equals("start time") || key[0].trim().toLowerCase().equals("test start time") || key[0].trim().toLowerCase()
										.equals("network 1time"))) {
									datatime[i - 1] = value[0];
								}
								absBodyMap.put(i, map);
							}
						} catch (ArrayIndexOutOfBoundsException e) {
							absBodyMap.put(key, "");
							System.out.println("filePath:  " + filePath.getAbsolutePath());
							errStr2.append(filePath.getAbsolutePath() + ":\r\n");
							errStr2.append(e.toString() + "\r\n");
							e.printStackTrace();
						} catch (Exception e) {
							absBodyMap.put(key, "");
							System.out.println("filePath:  " + filePath.getAbsolutePath());
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
					if (!header.trim().equals("") && !testScenarip.trim().equals("")) {
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
						if (!body.trim().equals("") && !header.trim().equals("")) {
							str.append(",");
						}
						str.append(header);
						if (!header.trim().equals("") && !testScenarip.trim().equals("")) {
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
					if (!header.trim().equals("") && !testScenarip.trim().equals("")) {
						str.append(",");
					}
					str.append(testScenarip);
				}
			}

		} catch (Exception e) {
			System.out.println("filePath:  " + filePath.getAbsolutePath());
			errStr2.append(filePath.getAbsolutePath() + ":\r\n");
			errStr2.append(e.toString() + "\r\n");
			e.printStackTrace();
		}
		System.out.println("开始writeToFile中格式化时间************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		try {
			// 格式化时间
			String dataLong[] = new String[datatime.length];
			for (int i = 0; i < datatime.length; i++) {
				String datatimeStr = datatime[i];
				Date date1 = null;
				try {
					if (datatimeStr == null || datatimeStr.isEmpty()) {
						dataLong[i] = "";
					} else {
						String timeStrs[] = pattern2.split(datatimeStr);
						StringBuffer rex = new StringBuffer("");
						for (int j = 0; j < timeStrs.length; j++) {
							rex.append(timeStrs[j]);
							rex.append(dateStr[j]);
						}
						SimpleDateFormat formatter = new SimpleDateFormat(rex.toString());
						date1 = formatter.parse(datatimeStr);
						dataLong[i] = Long.toString(date1.getTime());
					}

				} catch (ParseException e) {
					e.printStackTrace();
					System.out.println("转换时间有误");
				}
			}
			System.out.println("keyspace:" + org);
			String imei = filePath.getParentFile().getName();
			setMapData(absHeaderMap, absBodyMap, testScenarioMap, numStrs[1], filePath.getAbsoluteFile(), dataLong, org, imei);

		} catch (Exception e) {
			System.out.println("filePath:  " + filePath.getAbsolutePath());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 处理报告大小
	 * 
	 * @param fileS
	 * @return
	 * @return String
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

	private String getLastModifies(File file) {
		long modifiedTime = file.lastModified();
		Date date = new Date(modifiedTime);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:MM");
		String dd = sdf.format(date);
		return dd;
	}

	/**
	 * 多次处理报告信息
	 * 
	 * @param headerMap
	 * @param bodyMap
	 * @param testScenrioMap
	 * @param numType
	 * @param file
	 * @param dataLong
	 * @param keyspace
	 * @param imei
	 * @return void
	 */
	private void setMapData(Map headerMap, Map bodyMap, Map testScenrioMap, String numType, File file, String[] dataLong, String keyspace, String imei) {
		
		System.out.println(keyspace+"     ::::::::setMapData");
		String oldNumType = "";

		String fileName = file.getName();
		String dateStrs[] = pattern1.split(fileName);
		String yearstr = dateStrs[1];
		String Tm[] = pattern2.split(yearstr);
		/**
		 * 2015年12月18日 15:56:54 修改时间
		 * 
		 */
		String testtime = Tm[0]+Tm[1]; //该时间为从文件名中截取出来的日期
		String testtimes = UtilDate.getMonth();
		//System.out.println(testtimes+"   :::::");
		/**
		 * 2016年1月6日 11:54:05修改
		 * 
		 * 匹配文件生成时间问题
		 */
		if(!testtime.equals(testtimes)){
			return;
		}
		if (sqlList != null && sqlList.size() >= 1) {
			oldNumType = sqlList.get(0).toString();
		} else {
			oldNumType = numType;
			sqlList.add(oldNumType);
		}
		if (sqlList.size() >= 10 || !oldNumType.equals(numType)) {
			dealData();
			sqlList.clear();
			sqlList.add(numType);
		}

		System.out.println("开始第一步SetMapData************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		List sqlList = new ArrayList();
		Map<StringBuffer, Boolean> resultMap = new HashMap<StringBuffer, Boolean>();
		String filePath = file.getAbsolutePath().replace(ConfParser.org_prefix.replace(File.separator, "|"), "");
		String fileSize = convertFileSize(file.length());
		String file_lastModifies = getLastModifies(file);
		Map allMap = new HashMap();
		String detailreport = "";
		if (dataLong == null || dataLong.length == 0) {
			dataLong = new String[] { "" };
		}

		// 获取detailreport
		String destFilePath = ConfParser.backReportPath;
		String path1 = filePath.substring(0, filePath.lastIndexOf("."));
		String filenameorg = path1.substring(path1.lastIndexOf("."), path1.length());
		String[] files = new String[] { filenameorg, ".detail", ".deviceInfo", ".monitor" };
		String[] filepaths = new String[4];
		if (detailreport.equals("")) {
			for (int i = 0; i < files.length; i++) {
				String newfilePath = file.getAbsolutePath().replace(filenameorg, files[i]);
				filepaths[i] = newfilePath;
				File newFile = new File(newfilePath);
				if (newFile.exists()) {
					detailreport += "1";
				} else {
					detailreport += "0";
				}
			}
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
		// 处理数据
		if (bodyMap != null) {
			if (bodyMap.size() == 1) {
				Map bodyNextMap = (Map) bodyMap.get(1);
				String fileIndex = imei + "|" + file.getName() + "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[0], imei);
				allMap = setMapData(bodyNextMap, numType, allMap, file, fileIndex, dataLong[0], imei);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				allMap.put("appid", appid);
				resultMap = resultsetDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, testtime);
				insertSqlInfo(resultMap);
			} else if (bodyMap.size() > 1) {
				for (int i = 0; i < bodyMap.size(); i++) {
					Map bodyNextMap = (Map) bodyMap.get(i + 1);
					String fileIndex = imei + "|" + file.getName() + "_" + i;
					allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[i], imei);
					allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[i], imei);
					allMap = setMapData(bodyNextMap, numType, allMap, file, fileIndex, dataLong[i], imei);
					allMap.put("fileName", fileName);
					allMap.put("filePath", filePath);
					allMap.put("fileSize", fileSize);
					allMap.put("file_lastModifies", file_lastModifies);
					allMap.put("appid", appid);
					// insertCassandraDB(lMap, numType, file, keyspace,0);
					resultMap = resultsetDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, testtime);
					// insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace,
					// numType, file, detailreport);
					insertSqlInfo(resultMap);
				}
				// }
			} else {
				String fileIndex = imei + "|" + file.getName() + "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[0], imei);
				allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[0], imei);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				allMap.put("appid", appid);
				// insertCassandraDB(allMap, numType, file, keyspace,0);
				resultMap = resultsetDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, testtime);
				// insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace,
				// numType, file, detailreport);
				insertSqlInfo(resultMap);
			}
		} else {
			String fileIndex = imei + "|" + file.getName() + "_" + 0;
			allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[0], imei);
			allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[0], imei);
			allMap.put("fileName", fileName);
			allMap.put("filePath", filePath);
			allMap.put("fileSize", fileSize);
			allMap.put("file_lastModifies", file_lastModifies);
			allMap.put("appid", appid);
			// insertCassandraDB(allMap, numType, file, keyspace,0);
			resultMap = resultsetDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, testtime);
			// insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType,
			// file, detailreport);
			insertSqlInfo(resultMap);
		}
		// 备份数据
		System.out.println("结束插入数据  开始备份文件************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		for (int i = 0; i < filepaths.length; i++) {
			String newfilePath = filepaths[i];
			if (newfilePath == null || newfilePath.isEmpty()) {
				continue;
			}
			File destFile = new File(newfilePath.replace(ConfParser.org_prefix, destFilePath));
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
	}

	/**
	 * 把sql转入sqllist
	 * 
	 * @param resultMap
	 * @return void
	 */
	private void insertSqlInfo(Map resultMap) {
		if (resultMap == null || resultMap.size() == 0) {
			return;
		}
		List sqlInfoList = new ArrayList();
		String sql = resultMap.keySet().iterator().next().toString();
		String flag = resultMap.values().iterator().next() + "";
		sqlInfoList.add(sql);
		sqlInfoList.add(flag);
		sqlList.add(sqlInfoList);
	}

	/*
	 * private void dealData(String testtime){ String numType =
	 * sqlList.get(0).toString(); String table = "";
	 * if(numType.equals("01001")){ table = "speed_test_"+testtime; }else
	 * if(numType.equals("02001")){ table = "web_browsing_"+testtime; }else
	 * if(numType.equals("02011")){ table = "video_test_"+testtime; }else
	 * if(numType.equals("03001") || numType.equals("03011")){ table =
	 * "http_test_"+testtime; }else if(numType.equals("04002")){ table =
	 * "ping_"+testtime; }else if(numType.equals("04006")){ table =
	 * "hand_over_"+testtime; }else if(numType.equals("05001")){ table =
	 * "call_test_"+testtime; }else if(numType.equals("05002")){ table =
	 * "sms_"+testtime; }else if(numType.equals("05005")){ table =
	 * "sms_query_"+testtime; }else { return; } String tableStr =
	 * "insert into "+table+" "; String insertSql = ""; String columnStr = "";
	 * String valuesStr = ""; List updateList = new ArrayList();
	 * 
	 * for (int i = 1; i < sqlList.size(); i++) { ArrayList list = (ArrayList)
	 * sqlList.get(i);
	 * 
	 * System.out.println(list+"  ::::::::::sqlList"); String sql = (String)
	 * list.get(0); String flag = list.get(1)+""; if(!sql.isEmpty()){
	 * if(flag.equals("false")){ if(sql.contains("values")){ String insertStr =
	 * sql.replace(tableStr, ""); if(columnStr.isEmpty()){
	 * 
	 * columnStr = insertStr.substring(0,insertStr.indexOf("values")+6);
	 * System.out.println(columnStr+"  :::::columnStr"); insertSql += tableStr;
	 * insertSql += columnStr;
	 * System.out.println(insertSql+"  ::::::::insertS"); } valuesStr =
	 * insertStr.substring(insertStr.indexOf("values")+6,insertStr.length())
	 * +","; insertSql += valuesStr; } }else{ updateList.add(sql); } } }
	 * while(insertSql.endsWith(",")){ insertSql = insertSql.substring(0,
	 * insertSql.length()-1); } boolean flag = true; try { start();
	 * if(!insertSql.isEmpty()){ System.out.println(insertSql);
	 * statement.execute(insertSql); } for (int i = 0; i < updateList.size();
	 * i++) { String updateSql =
	 * updateList.get(i)==null?"":updateList.get(i).toString();
	 * if(updateSql.isEmpty()){ continue; } System.out.println(updateSql);
	 * statement.execute(updateSql); } } catch (Exception e) { flag = false;
	 * e.printStackTrace(); }finally{ try { close(); } catch (SQLException e) {
	 * e.printStackTrace(); } }
	 * System.out.println("更新数据"+(flag==true?"成功":"失败")); }
	 */
	/**
	 * 批量处理sql
	 * 
	 * @return void
	 */
	private void dealData() {
		boolean flag = true;
		String url = ResultsetTestDao.dsturl.substring(0, ResultsetTestDao.dsturl.lastIndexOf("/") + 1) + basename;
		try {
			resultsetDao.start(url, ResultsetTestDao.dstuser, ResultsetTestDao.dstpassword);
			ResultsetTestDao.conn.setAutoCommit(false);
			for (int i = 1; i < sqlList.size(); i++) {
				// System.out.println(iterator.next()+"  iterator.next() ");
				ArrayList list = (ArrayList) sqlList.get(i);

				String sql = (String) list.get(0);
				System.out.println(sql + "  ::::::");
				// count++;
				
				ResultsetTestDao.statement.addBatch((String) sql);
				// 10000条记录插入一次
				count++;
				if (count % 50000 == 0) {
					ResultsetTestDao.statement.executeBatch();
					ResultsetTestDao.conn.commit();
				}
			}
			// 不足10000的最后进行插入
			ResultsetTestDao.statement.executeBatch();
			ResultsetTestDao.conn.commit();

		} catch (Exception e) {
			flag = false;
			e.printStackTrace();
		} finally {
			try {
				resultsetDao.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("更新数据" + (flag == true ? "成功" : "失败"));
	}

	/**
	 * 正常入app报告
	 * 
	 * @param DataMap
	 * @param numType
	 * @param allMap
	 * @param file
	 * @param fileIndex
	 * @param dataLong
	 * @param imei
	 * @return
	 * @return Map
	 */
	private Map setMapData(Map DataMap, String numType, Map allMap, File file, String fileIndex, String dataLong, String imei) {
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
			System.out.println("年月日入库错误：" + dataLong);
		}
		// System.out.println("fileIndex:"+fileIndex+",dataLong:"+dataLong+",yearMonthDay:"+dateStr+",imei:"+imei);
		allMap.put("file_index", fileIndex);
		allMap.put("time", dataLong);
		allMap.put("imei", imei);
		return allMap;
	}

	// --------------------------------------------数据库连接处理--------------------------------------------------------

	// ---------------------------------------地理位置信息处理---------------------------------------------

	public String subTxt(String str) {
		if (str.startsWith("{")) {
			str = str.substring(1, str.length());
		}
		if (str.endsWith("}")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
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

	// ---------------------------------------文件解析处理------------------------------------------------

	/**
	 * 读取CSV文件
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

	// ------------------------------------------------------------------------------------------------
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

	public ResultsetTestData() {
		super();
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	public ResultsetTestData(String root) {
		super();
		this.root = root;
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}
}
