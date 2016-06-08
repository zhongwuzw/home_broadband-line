package com.opencassandra.test;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

/**
 * 1. 根据经纬度按块划分，将全国1-12月的数据导出成csv文件；
 * 2. 将导出的测试报告数据在原有格式基础上增加两列“终端类型”和“制造商”；
 * @author wuxiaofeng
 *
 */
public class IntoCSVByEara {

	private String csvFilePath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvstest";
	private static Map<Integer, LBBean> LBCodeMap = new HashMap<Integer, LBBean>();
	private static Map<String, String> deviceMap = new HashMap<String, String>();
	
	private static Map<Integer, List<String[]>> resultMap = new HashMap<Integer, List<String[]>>();
	private static List<String[]> otherResultList = new ArrayList<String[]>();
	private static Runtime r = Runtime.getRuntime();
	private static long total = r.totalMemory();
	
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	
	private int count = 0;
	
	static { 
		LBBean lb1 = new LBBean(97, 101, 50, 54);
		LBCodeMap.put(1, lb1);
		LBBean lb2 = new LBBean(101, 105, 50, 54);
		LBCodeMap.put(2, lb2);
		LBBean lb3 = new LBBean(105, 109, 50, 54);
		LBCodeMap.put(3, lb3);
		LBBean lb4 = new LBBean(109, 113, 50, 54);
		LBCodeMap.put(4, lb4);
		LBBean lb5 = new LBBean(113, 117, 50, 54);
		LBCodeMap.put(5, lb5);
		LBBean lb6 = new LBBean(117, 121, 50, 54);
		LBCodeMap.put(6, lb6);
		LBBean lb7 = new LBBean(121, 125, 50, 54);
		LBCodeMap.put(7, lb7);
		LBBean lb8 = new LBBean(125, 129, 50, 54);
		LBCodeMap.put(8, lb8);
		LBBean lb9 = new LBBean(129, 133, 50, 54);
		LBCodeMap.put(9, lb9);
		LBBean lb10 = new LBBean(133, 137, 50, 54);
		LBCodeMap.put(10, lb10);

		LBBean lb11 = new LBBean(97, 101, 46, 50);
		LBCodeMap.put(11, lb11);
		LBBean lb12 = new LBBean(101, 105, 46, 50);
		LBCodeMap.put(12, lb12);
		LBBean lb13 = new LBBean(105, 109, 46, 50);
		LBCodeMap.put(13, lb13);
		LBBean lb14 = new LBBean(109, 113, 46, 50);
		LBCodeMap.put(14, lb14);
		LBBean lb15 = new LBBean(113, 117, 46, 50);
		LBCodeMap.put(15, lb15);
		LBBean lb16 = new LBBean(117, 121, 46, 50);
		LBCodeMap.put(16, lb16);
		LBBean lb17 = new LBBean(121, 125, 46, 50);
		LBCodeMap.put(17, lb17);
		LBBean lb18 = new LBBean(125, 129, 46, 50);
		LBCodeMap.put(18, lb18);
		LBBean lb19 = new LBBean(129, 133, 46, 50);
		LBCodeMap.put(19, lb19);
		LBBean lb20 = new LBBean(133, 137, 46, 50);
		LBCodeMap.put(20, lb20);

		LBBean lb21 = new LBBean(97, 101, 42, 46);
		LBCodeMap.put(21, lb21);
		LBBean lb22 = new LBBean(101, 105, 42, 46);
		LBCodeMap.put(22, lb22);
		LBBean lb23 = new LBBean(105, 109, 42, 46);
		LBCodeMap.put(23, lb23);
		LBBean lb24 = new LBBean(109, 113, 42, 46);
		LBCodeMap.put(24, lb24);
		LBBean lb25 = new LBBean(113, 117, 42, 46);
		LBCodeMap.put(25, lb25);
		LBBean lb26 = new LBBean(117, 121, 42, 46);
		LBCodeMap.put(26, lb26);
		LBBean lb27 = new LBBean(121, 125, 42, 46);
		LBCodeMap.put(27, lb27);
		LBBean lb28 = new LBBean(125, 129, 42, 46);
		LBCodeMap.put(28, lb28);
		LBBean lb29 = new LBBean(129, 133, 42, 46);
		LBCodeMap.put(29, lb29);
		LBBean lb30 = new LBBean(133, 137, 42, 46);
		LBCodeMap.put(30, lb30);

		LBBean lb31 = new LBBean(97, 101, 38, 42);
		LBCodeMap.put(31, lb31);
		LBBean lb32 = new LBBean(101, 105, 38, 42);
		LBCodeMap.put(32, lb32);
		LBBean lb33 = new LBBean(105, 109, 38, 42);
		LBCodeMap.put(33, lb33);
		LBBean lb34 = new LBBean(109, 113, 38, 42);
		LBCodeMap.put(34, lb34);
		LBBean lb35 = new LBBean(113, 117, 38, 42);
		LBCodeMap.put(35, lb35);
		LBBean lb36 = new LBBean(117, 121, 38, 42);
		LBCodeMap.put(36, lb36);
		LBBean lb37 = new LBBean(121, 125, 38, 42);
		LBCodeMap.put(37, lb37);
		LBBean lb38 = new LBBean(125, 129, 38, 42);
		LBCodeMap.put(38, lb38);
		LBBean lb39 = new LBBean(129, 133, 38, 42);
		LBCodeMap.put(39, lb39);
		LBBean lb40 = new LBBean(133, 137, 38, 42);
		LBCodeMap.put(40, lb40);

		LBBean lb41 = new LBBean(97, 101, 34, 38);
		LBCodeMap.put(41, lb41);
		LBBean lb42 = new LBBean(101, 105, 34, 38);
		LBCodeMap.put(42, lb42);
		LBBean lb43 = new LBBean(105, 109, 34, 38);
		LBCodeMap.put(43, lb43);
		LBBean lb44 = new LBBean(109, 113, 34, 38);
		LBCodeMap.put(44, lb44);
		LBBean lb45 = new LBBean(113, 117, 34, 38);
		LBCodeMap.put(45, lb45);
		LBBean lb46 = new LBBean(117, 121, 34, 38);
		LBCodeMap.put(46, lb46);
		LBBean lb47 = new LBBean(121, 125, 34, 38);
		LBCodeMap.put(47, lb47);
		LBBean lb48 = new LBBean(125, 129, 34, 38);
		LBCodeMap.put(48, lb48);
		LBBean lb49 = new LBBean(129, 133, 34, 38);
		LBCodeMap.put(49, lb49);
		LBBean lb50 = new LBBean(133, 137, 34, 38);
		LBCodeMap.put(50, lb50);

		LBBean lb51 = new LBBean(97, 101, 30, 34);
		LBCodeMap.put(51, lb51);
		LBBean lb52 = new LBBean(101, 105, 30, 34);
		LBCodeMap.put(52, lb52);
		LBBean lb53 = new LBBean(105, 109, 30, 34);
		LBCodeMap.put(53, lb53);
		LBBean lb54 = new LBBean(109, 113, 30, 34);
		LBCodeMap.put(54, lb54);
		LBBean lb55 = new LBBean(113, 117, 30, 34);
		LBCodeMap.put(55, lb55);
		LBBean lb56 = new LBBean(117, 121, 30, 34);
		LBCodeMap.put(56, lb56);
		LBBean lb57 = new LBBean(121, 125, 30, 34);
		LBCodeMap.put(57, lb57);
		LBBean lb58 = new LBBean(125, 129, 30, 34);
		LBCodeMap.put(58, lb58);
		LBBean lb59 = new LBBean(129, 133, 30, 34);
		LBCodeMap.put(59, lb59);
		LBBean lb60 = new LBBean(133, 137, 30, 34);
		LBCodeMap.put(60, lb60);

		LBBean lb61 = new LBBean(97, 101, 26, 30);
		LBCodeMap.put(61, lb61);
		LBBean lb62 = new LBBean(101, 105, 26, 30);
		LBCodeMap.put(62, lb62);
		LBBean lb63 = new LBBean(105, 109, 26, 30);
		LBCodeMap.put(63, lb63);
		LBBean lb64 = new LBBean(109, 113, 26, 30);
		LBCodeMap.put(64, lb64);
		LBBean lb65 = new LBBean(113, 117, 26, 30);
		LBCodeMap.put(65, lb65);
		LBBean lb66 = new LBBean(117, 121, 26, 30);
		LBCodeMap.put(66, lb66);
		LBBean lb67 = new LBBean(121, 125, 26, 30);
		LBCodeMap.put(67, lb67);
		LBBean lb68 = new LBBean(125, 129, 26, 30);
		LBCodeMap.put(68, lb68);
		LBBean lb69 = new LBBean(129, 133, 26, 30);
		LBCodeMap.put(69, lb69);
		LBBean lb70 = new LBBean(133, 137, 26, 30);
		LBCodeMap.put(70, lb70);

		LBBean lb71 = new LBBean(97, 101, 22, 26);
		LBCodeMap.put(71, lb71);
		LBBean lb72 = new LBBean(101, 105, 22, 26);
		LBCodeMap.put(72, lb72);
		LBBean lb73 = new LBBean(105, 109, 22, 26);
		LBCodeMap.put(73, lb73);
		LBBean lb74 = new LBBean(109, 113, 22, 26);
		LBCodeMap.put(74, lb74);
		LBBean lb75 = new LBBean(113, 117, 22, 26);
		LBCodeMap.put(75, lb75);
		LBBean lb76 = new LBBean(117, 121, 22, 26);
		LBCodeMap.put(76, lb76);
		LBBean lb77 = new LBBean(121, 125, 22, 26);
		LBCodeMap.put(77, lb77);
		LBBean lb78 = new LBBean(125, 129, 22, 26);
		LBCodeMap.put(78, lb78);
		LBBean lb79 = new LBBean(129, 133, 22, 26);
		LBCodeMap.put(79, lb79);
		LBBean lb80 = new LBBean(133, 137, 22, 26);
		LBCodeMap.put(80, lb80);

		LBBean lb81 = new LBBean(97, 101, 18, 22);
		LBCodeMap.put(81, lb81);
		LBBean lb82 = new LBBean(101, 105, 18, 22);
		LBCodeMap.put(82, lb82);
		LBBean lb83 = new LBBean(105, 109, 18, 22);
		LBCodeMap.put(83, lb83);
		LBBean lb84 = new LBBean(109, 113, 18, 22);
		LBCodeMap.put(84, lb84);
		LBBean lb85 = new LBBean(113, 117, 18, 22);
		LBCodeMap.put(85, lb85);
		LBBean lb86 = new LBBean(117, 121, 18, 22);
		LBCodeMap.put(86, lb86);
		LBBean lb87 = new LBBean(121, 125, 18, 22);
		LBCodeMap.put(87, lb87);
		LBBean lb88 = new LBBean(125, 129, 18, 22);
		LBCodeMap.put(88, lb88);
		LBBean lb89 = new LBBean(129, 133, 18, 22);
		LBCodeMap.put(89, lb89);
		LBBean lb90 = new LBBean(133, 137, 18, 22);
		LBCodeMap.put(90, lb90);

		LBBean lb91 = new LBBean(97, 101, 14, 18);
		LBCodeMap.put(91, lb91);
		LBBean lb92 = new LBBean(101, 105, 14, 18);
		LBCodeMap.put(92, lb92);
		LBBean lb93 = new LBBean(105, 109, 14, 18);
		LBCodeMap.put(93, lb93);
		LBBean lb94 = new LBBean(109, 113, 14, 18);
		LBCodeMap.put(94, lb94);
		LBBean lb95 = new LBBean(113, 117, 14, 18);
		LBCodeMap.put(95, lb95);
		LBBean lb96 = new LBBean(117, 121, 14, 18);
		LBCodeMap.put(96, lb96);
		LBBean lb97 = new LBBean(121, 125, 14, 182);
		LBCodeMap.put(97, lb97);
		LBBean lb98 = new LBBean(125, 129, 14, 18);
		LBCodeMap.put(98, lb98);
		LBBean lb99 = new LBBean(129, 133, 14, 18);
		LBCodeMap.put(99, lb99);
		LBBean lb100 = new LBBean(133, 137, 14, 18);
		LBCodeMap.put(100, lb100);
		
		LBBean lb101 = new LBBean(73, 97, 14, 54);
		LBCodeMap.put(101, lb101);
		
		Connection conn = getConn();
		try {
			String sql = "SELECT * FROM `d_device` d LEFT JOIN b_device_type b on d.device_type_id=b.device_type_id LEFT JOIN b_mfr m on b.mfr_id=m.mfr_id;";
			PreparedStatement stmt= conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String deviceImei = rs.getString("device_imei").trim();
				String deviceModel = rs.getString("device_model").trim();
				String mfrName = rs.getString("mfr_name").trim();
//				System.out.println(deviceImei + "," + deviceModel + "," + mfrName);
				deviceMap.put(deviceImei, deviceModel + "," + mfrName);
			}
		} catch (SQLException e) {
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public ArrayList<String[]> readeDetailCsvUtf(String filePath) {
		
		ArrayList<String[]> csvList = null;
		CsvReader reader = null;
		try {
			csvList = new ArrayList<String[]>();
			String csvFilePath = filePath;
			reader = new CsvReader(csvFilePath, ',',
					Charset.forName("UTF-8")); 
			while (reader.readRecord()) {
				csvList.add(reader.getValues());
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if(reader!=null){
				reader.close();
			}
		}
		return csvList;
	}

	public void analyzeCsv(File myFile) {
		
		if (r.freeMemory() <= total * 0.5 ) {
			for (int code : resultMap.keySet()) {
				openCsv(csvFilePath + File.separator + "result_" + code + "(" + LBCodeMap.get(code).getLongtitudeStart() + "," + LBCodeMap.get(code).getLatitudeStart() + ").csv");
				List<String[]> resultList = resultMap.get(code);
				for (String[] result : resultList) {
					writeCsv(result);
				}
				closeCsv();
			}
			resultMap.clear();
			
			openCsv(csvFilePath + File.separator + "result_other.csv");
			for (String[] otherResult : otherResultList) {
				writeCsv(otherResult);
			}
			closeCsv();
			otherResultList.clear();
		}
		
		ArrayList<String[]> csvList = readeDetailCsvUtf(myFile.getAbsolutePath());
		
		for (int i = 0; i < csvList.size(); i++) {
			count++;
			if (count % 1000 == 0) {
				System.out.println("Dealed With "+count+" Datas");
			}
			
			String[] contents = csvList.get(i);
			String longtitude = "";
			String latitude = "";
			String imeiInfo = "";
			String imei = "";
			double longtitudeD = 0;
			double latitudeD = 0;
			if (contents != null) {
				longtitude = contents[2] != null ? contents[2] : "";   //经度
				latitude = contents[3] != null ? contents[3] : "";   //维度
				imeiInfo = contents[4] !=null ? contents[4] : "";
			}
			try {
				if (imeiInfo.equals("")) {
					continue;
				} else {
					imei = imeiInfo.substring(0, imeiInfo.indexOf("|"));
				}
			} catch (Exception e) {
				otherResultList.add(contents);
				System.out.println("Paser Exception: imeiInfo-->" + imeiInfo);
				continue;
			}
			
			if (imei != null && !"".equals(imei)) {
				try {
					if (longtitude != null && !"".equals(longtitude)) {
						longtitudeD = Double.parseDouble(longtitude);
					}
					if (latitude != null && !"".equals(latitude)) {
						latitudeD = Double.parseDouble(latitude);
					}
				} catch (Exception e) {
					otherResultList.add(contents);
					System.out.println("Paser Exception: longtitude-->"+longtitude+" latitude-->"+latitude);
					continue;
				}
				
				boolean flag = false;
				for (int lbCode : LBCodeMap.keySet()) {
					LBBean lbBean = LBCodeMap.get(lbCode);
					if (longtitudeD >= lbBean.getLongtitudeStart() && longtitudeD < lbBean.getLongtitudeEnd() && latitudeD >= lbBean.getLatitudeStart() && latitudeD < lbBean.getLatitudeEnd()) {

						flag = true;
						int index = (int)((longtitudeD - lbBean.getLongtitudeStart()) / 0.1) * 10 + (int)((latitudeD - lbBean.getLatitudeStart()) / 0.05);
						String[] result = new String[27];
						for (int l = 0; l < contents.length; l++) {
							result[l] = contents[l];
							if (l == contents.length - 1) {
								l++;
								for(; l < result.length; l++) {
									result[l] = "";
								}
							}
						}
						result[24] = index+"";
						try {
							String[] devices = deviceMap.get(imei.trim()).split(",");
							result[25] = devices[0];
							result[26] = devices[1];
						} catch (Exception e) {
							otherResultList.add(result);
							System.out.println("=====imei:" + imei);
							continue;
						}
						
						if (resultMap.get(lbCode) == null) {
							List<String[]> list = new ArrayList<String[]>();
							list.add(result);
							resultMap.put(lbCode, list);
						} else {
							resultMap.get(lbCode).add(result);
						}
						break;
					}
				}
				if (flag == false) {
					otherResultList.add(contents);
				}
			}
		}
	}
	
	public void openCsv(String path) {
		
		csvFile = new File(path);
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
			}
			writer = new BufferedWriter(new FileWriter(csvFile,true));
			cwriter = new CsvWriter(writer,',');
		} catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void writeCsv(String[] contents) {
		try {
			cwriter.writeRecord(contents, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeCsv() {
		try{
			if(writer!=null){
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(cwriter!=null){
				cwriter.flush();
				cwriter.close();
			}
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
	static public void main(String[] args) {
			
		Date start = new Date();
		System.out.println("Start:"+start.toLocaleString());
		IntoCSVByEara mydwc = new IntoCSVByEara();
		mydwc.findFile("C:\\Users\\issuser\\Desktop\\云测试平台项目\\日志及cvs文档\\按经纬度拆分的文件\\result");
			
		for (int code : resultMap.keySet()) {
			mydwc.openCsv(mydwc.csvFilePath + File.separator + "result_" + code + "(" + LBCodeMap.get(code).getLongtitudeStart() + "," + LBCodeMap.get(code).getLatitudeStart() + ").csv");
			List<String[]> resultList = resultMap.get(code);
			for (String[] result : resultList) {
				mydwc.writeCsv(result);
			}
			mydwc.closeCsv();
		}
		Date end = new Date();
		System.out.println("Start:"+start.toLocaleString()+">>> End:"+end.toLocaleString());
	}
	
	public void findFile(String rootDirectory){
		
		File[] list = (new File(rootDirectory)).listFiles();
		for(int i=0; i<list.length; i++){
			if(list[i].isDirectory()){
				findFile(list[i].getAbsolutePath());
			}else{
				System.out.println("正在读取文件夹中第"+(i+1)+"个文件，共"+list.length+"个文件");
				this.analyzeCsv(list[i]);
				System.out.println(list[i].getAbsolutePath());
			}
		}
	}
	
	private static Connection getConn() {
		
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.85.234:3306/device";
		String user = "root";
		String password = "cmrictpdata";
		try {
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			return conn;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
