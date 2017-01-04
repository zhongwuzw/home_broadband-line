package com.opencassandra.resultsettest.separateapp_zhongce;

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

import net.sf.json.JSONObject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import com.csvreader.CsvReader;
import com.opencassandra.descfile.ConfParser;

public class PCResultTestData {
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
	private static ResultsetTestDao insertMysqlDao = new ResultsetTestDao();
	private List appid = new ArrayList();	//Appid
	private List sqlList = new ArrayList();
	static String basename = "";
	static Statement statement;
	static Connection conn;
	// 截取报告入库时间的rex
	private static Pattern pattern1 = Pattern.compile("[-]");
	private static Pattern pattern2 = Pattern.compile("[_]");
	static List<String> filepathList = new ArrayList<String>();
	public static int getCount() {
		return count;
	}

	synchronized public static void setCount() {
		PCResultTestData.count++;
	}

	public static int successCount = 0;
	public static int failCount = 0;
	private ExecutorService analyzeThreadPool;
	
	//拼接自然维路径
	static public List<String> getPaths(String[] path){
		List pathList = new ArrayList<String>();
		String prefix = ConfParser.org_prefix;
		if((path!=null && path.length>0)){
			for (int i = 0; i < path.length; i++) {
				String org_keyString = path[i].toString();
				if(!org_keyString.isEmpty()){
					pathList.add(prefix+org_keyString);
				}
			}
		}
		return pathList;
	}

	/**
	 * @override
	 */
	public void run() {
		System.out.println("counts: " + this.getCounts());
	}
	
	static public void main(String[] args) {
		String testtime = "";
		if (ConfParser.time != null && !ConfParser.time.equals("")
				&& ConfParser.time != "") {
			testtime = ConfParser.time;
		} else {
			if (args.length > 0) {
				testtime = args[0];
			}
		}

		PCResultTestData desc = new PCResultTestData();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		System.out.println(dateString + ">>> Start!");
		desc.setChange(true);
		desc.appid = desc.getAppids();
		System.out.println("开始获取路径进行遍历" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		String code1 = (ConfParser.code != null && ConfParser.code.length > 0) ? ConfParser.code[0] : "";
		
		String[] prjcode = ConfParser.prjcode;
		if (prjcode != null) {
			List<String> pathList = PCResultTestData.getPaths(prjcode);
			for (int i = 0; i < pathList.size(); i++) {
				String filePath = pathList.get(i);
				System.out.println(filePath);
				String org = prjcode[i];
				desc.findFile(filePath, org, testtime, ConfParser.code);
			}
		}

		//将sql插入数据库
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

	//通过app中文名查找APPID
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
			appids = insertMysqlDao.getappIds(sql);
			if (flag) {
				appids.add("default_app_id");
			}
			return appids;
		}
	}
	
    public String readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        StringBuffer sb = new StringBuffer();
        try {
           // reader = new BufferedReader(new FileReader(file));
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file),"GB2312"));
            String tempString = null;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                //System.out.println("line " + line + ": " + tempString);
                sb.append(tempString);
                sb.append("\r");
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
		return sb.toString();
    }

	//进行文件搜索
	public void findFile(String rootDirectory, String org,String testtime, String[] code) {
		System.out.println("开始findFile************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		File rootFile = new File(rootDirectory);
		if (!rootFile.exists()) {
			return;
		}
		File[] fileList = rootFile.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				File file = new File(dir,name);
				if (file.isDirectory() || name.indexOf(".resource.csv") >= 0 || name.indexOf(".summary.csv") >= 0 || name.indexOf(".abstract.csv") >= 0) {
					return true;
				}
				return false;
			}
		});
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = dateFormat.format(new Date());
		StringBuffer errStr = new StringBuffer();
		try {
			//fileList数组包含目录，以及csv文件
			for (int i = 0; i < fileList.length; i++) {
				File myFile = fileList[i];
				System.out.println(myFile.getAbsolutePath() + "  ::::::");
				if (myFile.isDirectory()) {
					length = length + myFile.listFiles().length;
					findFile(myFile.getAbsolutePath(), org,testtime, code);
				} else {
					String[] fileComponents = myFile.toString().split(Matcher.quoteReplacement(System.getProperty("file.separator")));
					if (fileComponents.length >= 5) {
						if (!testtime.equals("") && !((String)fileComponents[fileComponents.length - 4]).equals(testtime)) {
							continue;
						}
						if (code != null) {
							boolean isContinue = true;
							for (int j = 0; j < code.length; j++) {
								if (code[j].equals(fileComponents[fileComponents.length - 5])) {
									isContinue = false;
									break;
								}
							}
							if (isContinue) {
								continue;
							}
						}
					}
					else {
						continue;
					}
					for (String filePath : filepathList) {
						if (filePath.equals(myFile.getAbsolutePath().toString())) {
							return;
						}
					}
					filepathList.add(myFile.getAbsolutePath());
					try {
						try {
							PCFileLog.writeToLogFile(myFile.toString());
							String fileContent = this.readFileByLines(myFile.toString());
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
							System.out.println("已处理报告数为：" + length);
						}
					} catch (Exception e) {
						e.printStackTrace();
						errStr.append(myFile.getAbsolutePath() + ":\r\n");
						errStr.append(e.getMessage() + "\r\n");
					}
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
	public void deal(File file, int ServiceId, String org,String testtime) {
		System.out.println(file.getAbsolutePath() + "   ::::::::::");
		if (file.getAbsoluteFile().toString().endsWith(".abstract.csv") || file.getAbsoluteFile().toString().endsWith(".summary.csv")) {
			System.out.println("开始deal************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
			System.out.println(org+"   deal device_org 的值");
			splitAbstractCsv(file, 01001, org,testtime);
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
	public boolean splitAbstractCsv(File filePath, int ServiceId, String org,String testtime) {
		System.out.println("开始splitAbstractCsv************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
		
		System.out.println(org+"   splitAbstractCsv device_org 的值");
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
				}//每次强制走01001
			} else if (ServiceId == 01001) {
				if (csvList == null || csvList.size() == 0) {
					return false;
				}
				for (int i = 0; i < csvList.size(); i++) {
					if (csvList.get(i) != null && !isBlankLine(csvList.get(i))) {
						if (csvList.get(i)[0] != null) {
							if ((csvList.get(i)[0].equals("测试时间") || csvList.get(i)[0].trim().toLowerCase().equals("time") || csvList.get(i)[0].trim().toLowerCase().equals("test time")
									|| csvList.get(i)[0].equals("Test URL")|| csvList.get(i)[0].equals("DstAddress"))
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
									.equals("address")))|| csvList.get(i)[0].equals("Test URL")|| csvList.get(i)[0].equals("DstAddress"))
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
			this.writeToFile(absHeader, absBody, testScenario, filePath, org,testtime);
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
	private void writeToFile(ArrayList<String[]> absHeader, ArrayList<String[]> absBody, ArrayList<String[]> testScenario, File filePath, String org,String testtime) {
		System.out.println("开始writeToFile************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
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
									
									if("文件".equals(sonKey) || "File".equals(sonKey) || "测试文件".equals(sonKey) || "上传地址".equals(sonKey)||"下载地址".equals(sonKey)
											|| "Test File".equals(sonKey) || "Download File".equals(sonKey) || "Upload File".equals(sonKey)){
										urladd[i-1] = sonValue;
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
		System.out.println(urladd);
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
			setMapData(absHeaderMap, absBodyMap, testScenarioMap, numStrs[1], filePath.getAbsoluteFile(), dataLong, org, imei,testtime,urladd);

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
	private void setMapData(Map headerMap, Map bodyMap, Map testScenrioMap, String numType, File file, String[] dataLong, String keyspace, String imei,String testtimes,String[] urladd) {
		String oldNumType = "";
		System.out.println(keyspace+"   setmapData device_org 的值");
		List list = new ArrayList();
		String fileName = file.getName();
		/*String dateStrs[] = pattern1.split(fileName);
		String yearstr = dateStrs[1];
		String Tm[] = pattern2.split(yearstr);*/
		/**
		 * 2015年12月18日 15:56:54 修改时间
		 * 
		 */
	/*	String testtime = Tm[0]+Tm[1]; //该时间为从文件名中截取出来的日期
		String testtimes = UtilDate.getMonth();*/
		//System.out.println(testtimes+"   :::::");
		
		String test_time = testtimes.substring(0,6);
		
		/**
		 * 2016年1月6日 11:54:05修改
		 * 
		 * 匹配文件生成时间问题
		 */
		/*if(!testtime.equals(testtimes)){
			return;
		}*/
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
				allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[0], imei,urladd[0]);
				allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[0], imei,urladd[0]);
				allMap = setMapData(bodyNextMap, numType, allMap, file, fileIndex, dataLong[0], imei,urladd[0]);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				allMap.put("appid", appid);
				list.add(insertMysqlDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, test_time));
//				insertSqlInfo(resultMap);
			} else if (bodyMap.size() > 1) {
				for (int i = 0; i < bodyMap.size(); i++) {
					Map bodyNextMap = (Map) bodyMap.get(i + 1);
					String fileIndex = imei + "|" + file.getName() + "_" + i;
					allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[i], imei,urladd[i]);
					allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[i], imei,urladd[i]);
					allMap = setMapData(bodyNextMap, numType, allMap, file, fileIndex, dataLong[i], imei,urladd[i]);
					allMap.put("fileName", fileName);
					allMap.put("filePath", filePath);
					allMap.put("fileSize", fileSize);
					allMap.put("file_lastModifies", file_lastModifies);
					allMap.put("appid", appid);
					// insertCassandraDB(lMap, numType, file, keyspace,0);
					list.add(insertMysqlDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, test_time));
					// insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace,
					// numType, file, detailreport);
//					insertSqlInfo(resultMap);
				}
				// }
			} else {
				String fileIndex = imei + "|" + file.getName() + "_" + 0;
				allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[0], imei,urladd[0]);
				allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[0], imei,urladd[0]);
				allMap.put("fileName", fileName);
				allMap.put("filePath", filePath);
				allMap.put("fileSize", fileSize);
				allMap.put("file_lastModifies", file_lastModifies);
				allMap.put("appid", appid);
				// insertCassandraDB(allMap, numType, file, keyspace,0);
				list.add(insertMysqlDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, test_time));
				// insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace,
				// numType, file, detailreport);
//				insertSqlInfo(resultMap);
			}
		} else {
			String fileIndex = imei + "|" + file.getName() + "_" + 0;
			allMap = setMapData(headerMap, numType, allMap, file, fileIndex, dataLong[0], imei,"");
			allMap = setMapData(testScenrioMap, numType, allMap, file, fileIndex, dataLong[0], imei,"");
			allMap.put("fileName", fileName);
			allMap.put("filePath", filePath);
			allMap.put("fileSize", fileSize);
			allMap.put("file_lastModifies", file_lastModifies);
			allMap.put("appid", appid);
			// insertCassandraDB(allMap, numType, file, keyspace,0);
			list.add(insertMysqlDao.insertMysqlDB(allMap, keyspace, numType, file, detailreport, test_time));
			// insertMysqlByDataToGPS.insertMysqlDB(allMap, keyspace, numType,
			// file, detailreport);
//			insertSqlInfo(resultMap);
		}
		sqlList.add(parsingData(list, file));
//		
//		// 备份数据
//		System.out.println("结束插入数据  开始备份文件************：" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
//		for (int i = 0; i < filepaths.length; i++) {
//			String newfilePath = filepaths[i];
//			if (newfilePath == null || newfilePath.isEmpty()) {
//				continue;
//			}
//			File destFile = new File(newfilePath.replace(ConfParser.org_prefix, destFilePath));
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
//		System.out.println("结束备份************：" +
//				"" + new Date().toLocaleString() + "   毫秒：" + new Date().getTime());
	}

	/**
	 * 处理数据
	 * 
	 * @param list
	 * @param file
	 * @return
	 */
	public List parsingData(List list, File file) {
		List sqls = new ArrayList();
		String sql = "";
		String table = "";
		if (list.size() > 0 && list != null) {
			try {
				table = ((Map) list.get(0)).values().iterator().next()
						.toString();
			} catch (Exception e) {
			}
			if (file != null) {
				sqls.add(file);
			}
			if (table != null) {
				sqls.add(table);
			}
			for (int i = 0; i < list.size(); i++) {
				Map resultMap = (Map) list.get(i);
				if (resultMap != null) {
					try {
						sql = resultMap.keySet().iterator().next().toString();
						sqls.add(sql);
					} catch (Exception e) {
						sqls.add("error");
					}
				}else{
					sqls.add("error");
				}
			}
			return sqls;
		}
		return null;
	}
//	/**
//	 * 把sql转入sqllist
//	 * 
//	 * @param resultMap
//	 * @return void
//	 */
//	private void insertSqlInfo(Map resultMap) {
//		if (resultMap == null || resultMap.size() == 0) {
//			return;
//		}
//		List sqlInfoList = new ArrayList();
//		String sql = resultMap.keySet().iterator().next().toString();
//		String flag =resultMap.values().iterator().next() + "";
//		sqlInfoList.add(sql);
//		sqlInfoList.add(flag);
//		sqlList.add(sqlInfoList);
//	}
	private void dealData() {
		boolean judge = true;
		boolean judgeFile = false;
		ResultsetTestDao rtd = new ResultsetTestDao();
		String url = ResultsetTestDao.dsturl.substring(0,
				ResultsetTestDao.dsturl.lastIndexOf("/") + 1) + basename;
		try {
			this.start(url, ResultsetTestDao.dstuser,
					ResultsetTestDao.dstpassword);
			for (int i = 1; i < sqlList.size(); i++) {
				judgeFile = true;
				List list = (List) sqlList.get(i);
				for (int j = 2; j < list.size(); j++) {
					try {
						String sql =list.get(j).toString();
						if ("error".equals(sql) || "error" == sql) {
							judgeFile =  false;
						}else{
							System.out.println("sql:\t" +sql);
							statement.execute(sql);
						}
					} catch (Exception e) {
						String table = list.get(1).toString();
						boolean flage = rtd.queryTabExist(table);
						String testtime = table.substring(table.length() - 6,
								table.length());
						String datatype = table
								.substring(0, table.length() - 7);
						if (!flage) {
							rtd.createTable(table);
							try {
								statement.execute(list.get(j).toString());
							} catch (Exception e2) {
								e2.printStackTrace();
								judgeFile = false;
							}
							String newtable = table.replace(testtime, "new_"
									+ testtime);
							boolean fl = rtd.queryTabExist(newtable);
							if (!fl) {
								rtd.createNewTable(newtable);
							}
							boolean opFla = rtd.queryTabExist("date_tablename");
							if (!opFla) {
								rtd.createDateTab("date_tablename");
							}
							boolean queryF = rtd
									.queryDataExist(table, newtable);
							if (!queryF) {
								String tabsql = "insert into date_tablename (confdate,tablename,newtablename,datatype)"
										+ " values('"
										+ testtime
										+ "','"
										+ table
										+ "','"
										+ newtable
										+ "','"
										+ datatype + "')";
								String u = ConfParser.dsturl.substring(0,
										ConfParser.dsturl.lastIndexOf("/") + 1)
										+ PCResultTestData.basename;
								rtd.start(u, ConfParser.dstuser,
										ConfParser.dstpassword);
								rtd.insert(tabsql);
								rtd.close();
							}
						}else{
							judgeFile =  false;
						}
					}
				}
					this.BackupFile((File) list.get(0), judgeFile);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				this.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 备份文件
	 * 
	 * @param file
	 */
	public void BackupFile(File file, boolean falg) {
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
//	/**
//	 * 批量处理sql
//	 * 
//	 * @return void
//	 */
//	private void dealData(String a) {
//		boolean flag = true;
//		String url = ResultsetTestDao.dsturl.substring(0, ResultsetTestDao.dsturl.lastIndexOf("/") + 1) + basename;
//		try {
//			insertMysqlDao.start(url, ResultsetTestDao.dstuser, ResultsetTestDao.dstpassword);
//			ResultsetTestDao.conn.setAutoCommit(false);
//			for (int i = 1; i < sqlList.size(); i++) {
//				// System.out.println(iterator.next()+"  iterator.next() ");
//				ArrayList list = (ArrayList) sqlList.get(i);
//
//				String sql = (String) list.get(0);
//				System.out.println(sql + "  ::::::");
//				// count++;
//				
//				ResultsetTestDao.statement.addBatch((String) sql);
//				// 10000条记录插入一次
//				count++;
//				if (count % 50000 == 0) {
//					ResultsetTestDao.statement.executeBatch();
//					ResultsetTestDao.conn.commit();
//				}
//			}
//			// 不足10000的最后进行插入
//			ResultsetTestDao.statement.executeBatch();
//			ResultsetTestDao.conn.commit();
//
//		} catch (Exception e) {
//			flag = false;
//			e.printStackTrace();
//		} finally {
//			try {
//				insertMysqlDao.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
//		System.out.println("更新数据" + (flag == true ? "成功" : "失败"));
//	}

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
	private Map setMapData(Map DataMap, String numType, Map allMap, File file, String fileIndex, String dataLong, String imei,String urladd) {
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
		allMap.put("url", urladd);
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

	public PCResultTestData() {
		super();
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
	}

	public PCResultTestData(String root) {
		super();
		this.root = root;
		analyzeThreadPool = Executors.newFixedThreadPool(1280);
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
	public void start(String url, String user, String password)
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
	public void close() throws SQLException {
		// TODO Auto-generated method stub
		if (statement != null) {
			statement.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
}
