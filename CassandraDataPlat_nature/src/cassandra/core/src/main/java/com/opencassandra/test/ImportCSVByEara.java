package com.opencassandra.test;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.opencassandra.descfile.ConfParser;

public class ImportCSVByEara {

//	String csvFilePath = "/ctp";
	String csvFilePath = "C:\\Users\\issuser\\Desktop\\云测试平台项目\\cvstest";
	String gpsCsvFilePath = "/ctp";
	CsvWriter wr = null;
	int count = 0;
	
	public ArrayList<String[]> readeDetailCsv(String filePath) {
		ArrayList<String[]> csvList = null;
		CsvReader reader = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath;
			reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
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
	
	public ArrayList<String[]> readeDetailCsvUtf(String filePath) {
		ArrayList<String[]> csvList = null;
		CsvReader reader = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath;
			reader = new CsvReader(csvFilePath, ',',
					Charset.forName("UTF-8")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
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
	
	public ArrayList<String[]> readeDetailCsvGbk(String filePath) {
		ArrayList<String[]> csvList = null;
		CsvReader reader = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath;
			reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GBK")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
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

	public void analyzeCsvByLB(File myFile, double longtitudeStart, double longtitudeEnd, double latitudeStart, double latitudeEnd) {
		
		ArrayList<String[]> csvList = readeDetailCsvUtf(myFile.getAbsolutePath());
		
		for (int i = 0; i < csvList.size(); i++) {
			count++;
			if (count % 1000 == 0) {
				System.out.println("Dealed With "+count+" Datas");
			}
			String[] contents = csvList.get(i);
			String longtitude = "";
			String latitude = "";
			double longtitudeD = 0;
			double latitudeD = 0;
			if (contents != null) {
				longtitude = contents[2] != null ? contents[2] : "";   //经度
				latitude = contents[3] != null ? contents[3] : "";   //维度
			}
			System.out.println("========longtitude:" + longtitude + "===========latitude:" + latitude);
			try {
				if (longtitude != null && !"".equals(longtitude)) {
					longtitudeD = Double.parseDouble(longtitude);
				}
				if (latitude != null && !"".equals(latitude)) {
					latitudeD = Double.parseDouble(latitude);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Paser Exception: longtitude-->"+longtitude+" latitude-->"+latitude);
				continue;
			}
			
			if (longtitudeD >= longtitudeStart && longtitudeD < longtitudeEnd && latitudeD >= latitudeStart && latitudeD < latitudeEnd) {

				int index = (int)((longtitudeD - longtitudeStart) / 0.1) * 10 + (int)((latitudeD - latitudeStart) / 0.05);
				System.out.println("===========index:" +index);
				String[] result = new String[25];
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
				this.writeCsv(result);
			}
		}
	}
	
	public void analyzeCsv(File myFile){
		
		ArrayList<String[]> csvList = readeDetailCsvUtf(myFile.getAbsolutePath());
		
		for(int i=0; i<csvList.size(); i++){
			count++;
			if(count%1000==0){
				System.out.println("Dealed With "+count+" Datas");
			}
			String[] contents = csvList.get(i);
			String longtitude = "";
			String latitude = "";
			double longtitudeD = 0;
			double latitudeD = 0;
			if(contents!=null){
				longtitude = contents[2]!=null?contents[2]:"";   //经度
				latitude = contents[3]!=null?contents[3]:"";   //维度
			}
			System.out.println("========longtitude:" + longtitude + "===========latitude:" + latitude);
			try{
				if (longtitude != null && !"".equals(longtitude)) {
					longtitudeD = Double.parseDouble(longtitude);
				}
				if (latitude != null && !"".equals(latitude)) {
					latitudeD = Double.parseDouble(latitude);
				}
			}catch(Exception e){
				e.printStackTrace();
				System.out.println("Paser Exception: longtitude-->"+longtitude+" latitude-->"+latitude);
				continue;
			}
			
			if(longtitudeD>=ConfParser.longtitudeStart && longtitudeD<ConfParser.longtitudeEnd && latitudeD>=ConfParser.latitudeStart && latitudeD<ConfParser.latitudeEnd){

				int index = (int)((longtitudeD-ConfParser.longtitudeStart)/0.1)*10+(int)((latitudeD-ConfParser.latitudeStart)/0.05);
				System.out.println("===========index:" +index);
				String[] result = new String[25];
				for(int l=0; l<contents.length; l++){
					result[l] = contents[l];
					if(l==contents.length-1){
						l++;
						for(;l<result.length;l++){
							result[l] = "";
						}
					}
				}
				result[24] = index+"";
				this.writeCsv(result);
			}
		}
	}
	
	/**
	 * 读取Detail.CSV文件
	 */
	public void analyzeGpsCsv(File myFile){
		
		ArrayList<String[]> csvList = readeDetailCsv(myFile.getAbsolutePath());
//		ArrayList<String[]> csvListUtf = readeDetailCsvUtf(myFile.getAbsolutePath());
//		ArrayList<String[]> csvListGbk = readeDetailCsvGbk(myFile.getAbsolutePath());
		
		for(int i=0; i<csvList.size(); i++){
			count++;
			if(count%1000==0){
				System.out.println("Dealed With "+count+" Datas");
			}
			String[] contents = csvList.get(i);
			String longtitude = "";
			String latitude = "";
			double longtitudeD = 0;
			double latitudeD = 0;
			if(contents!=null){
				longtitude = contents[2]!=null?contents[2]:"";
				latitude = contents[3]!=null?contents[3]:"";
			}
			try{
				longtitudeD = Double.parseDouble(longtitude);
				latitudeD = Double.parseDouble(latitude);
			}catch(Exception e){
				e.printStackTrace();
				System.out.println("Paser Exception: longtitude-->"+longtitude+" latitude-->"+latitude);
				continue;
			}
			
			String resp = "";
			try {
				resp = testPost(latitudeD+"", longtitudeD+"");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				this.writeCsv(contents);
				continue;
			}
			String spaceGpsStr = subLalo(resp);
			
			String spaces[] = spaceGpsStr.split("_");
			//String spaceName [] = new String[]{"province","city","district","street","street_number"};
			String[] result = new String[30];

			for(int l=0; l<contents.length; l++){
				result[l] = contents[l];
				if(l==contents.length-1){
					l++;
					for(;l<result.length;l++){
						result[l] = "";
					}
				}
			}
			
			for (int j = 0; j < spaces.length; j++) {
				result[25+j] = spaces[j]!=null?spaces[j]:"";
			}
			String re1Utf = "";
			String re1GBK = "";
			String re1GB2312 = "";
			try {
				byte[] rre1 = result[1].getBytes("GBK");
				re1Utf = new String(rre1,"utf-8");
				re1GBK = new String(rre1,"GBK");
				re1GB2312 = new String(rre1,"GB2312");
//				System.out.println("result[1]---->"+result[1]+" re1Utf---->"+re1Utf+"re1GBK---->"+re1GBK+"re1GB2312---->"+re1GB2312+"  result[25]---->"+result[25]);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			
//			String[] contentsUtf = csvListUtf.get(i); 
//			
//			try {
//				byte[] rre1 = contentsUtf[1].getBytes("GBK");
//				re1Utf = new String(rre1,"utf-8");
//				re1GBK = new String(rre1,"GBK");
//				re1GB2312 = new String(rre1,"GB2312");
//				System.out.println("contentsUtf[1]---->"+contentsUtf[1]+" re1Utf---->"+re1Utf+"re1GBK---->"+re1GBK+"re1GB2312---->"+re1GB2312+"  result[25]---->"+result[25]);
//			} catch (UnsupportedEncodingException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			
//			String[] contentsGbk = csvListGbk.get(i); 
//			try {
//				byte[] rre1 = contentsGbk[1].getBytes("GBK");
//				re1Utf = new String(rre1,"utf-8");
//				re1GBK = new String(rre1,"GBK");
//				re1GB2312 = new String(rre1,"GB2312");
//				System.out.println("contentsGbk[1]---->"+contentsGbk[1]+" re1Utf---->"+re1Utf+"re1GBK---->"+re1GBK+"re1GB2312---->"+re1GB2312+"  result[25]---->"+result[25]);
//			} catch (UnsupportedEncodingException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			this.writeCsv(result);
		}
	}
	
	/**
	 * 写入CSV文件
	 */
	public void openCsv(String path) {
		wr = new CsvWriter(path, ',',
				Charset.forName("GB2312"));
	}
	
	public void writeCsv(String[] contents) {
		try {
			wr.writeRecord(contents);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeCsv() {
		if(wr!=null){
			wr.close();
		}
	}
	
	static public void main(String[] args) {
			
		Date start = new Date();
		System.out.println("Start:"+start.toLocaleString());
//		int index = (int)((116.3506622-116.1)/0.1)*10+(int)((39.8975296-39.7)/0.05);
//		System.out.println(index);
		String operateState = "";
		if(args!=null&&args.length>=1){
			operateState = args[0];
		}
		
		if(operateState!=null && operateState.toLowerCase().equals("lbs")){
			ImportCSVByEara mydwc = new ImportCSVByEara();
			mydwc.openCsv(mydwc.csvFilePath+File.separator+"result_beijing_gps_"+(new Date()).getTime()+".csv");
			mydwc.findGpsFile("/home/user/liugang/OTS/result_deal_gps");
			mydwc.closeCsv();
		}else{
			ImportCSVByEara mydwc = new ImportCSVByEara();
//			mydwc.openCsv(mydwc.csvFilePath+File.separator+"result_beijing_"+(new Date()).getTime()+".csv");
//			mydwc.findFile("/home/user/liugang/OTS/result_deal");
//			mydwc.closeCsv();
			mydwc.openCsv(mydwc.csvFilePath+File.separator+"result_shanghai_"+(new Date()).getTime()+".csv");
			mydwc.findFileByLB("C:\\Users\\issuser\\Desktop\\云测试平台项目\\日志及cvs文档\\按经纬度拆分的文件\\result", ConfParser.ShangHaiLongtitudeStart, ConfParser.ShangHaiLongtitudeEnd, ConfParser.ShangHaiLatitudeStart, ConfParser.ShangHaiLatitudeEnd);
			mydwc.closeCsv();
			mydwc.openCsv(mydwc.csvFilePath+File.separator+"result_nanning_"+(new Date()).getTime()+".csv");
			mydwc.findFileByLB("C:\\Users\\issuser\\Desktop\\云测试平台项目\\日志及cvs文档\\按经纬度拆分的文件\\result", ConfParser.NanNingLongtitudeStart, ConfParser.NanNingLongtitudeEnd, ConfParser.NanNingLatitudeStart, ConfParser.NanNingLatitudeEnd);
			mydwc.closeCsv();
			mydwc.openCsv(mydwc.csvFilePath+File.separator+"result_chongqing_"+(new Date()).getTime()+".csv");
			mydwc.findFileByLB("C:\\Users\\issuser\\Desktop\\云测试平台项目\\日志及cvs文档\\按经纬度拆分的文件\\result", ConfParser.ChongQingLongtitudeStart, ConfParser.ChongQingLongtitudeEnd, ConfParser.ChongQingLatitudeStart, ConfParser.ChongQingLatitudeEnd);
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
	
	public void findFileByLB(String rootDirectory, double longtitudeStart, double longtitudeEnd, double latitudeStart, double latitudeEnd){
		
		File[] list = (new File(rootDirectory)).listFiles();
		for(int i=0; i<list.length; i++){
			if(list[i].isDirectory()){
				findFile(list[i].getAbsolutePath());
			}else{
				System.out.println("正在读取文件夹中第"+(i+1)+"个文件，共"+list.length+"个文件");
				this.analyzeCsvByLB(list[i], longtitudeStart, longtitudeEnd, latitudeStart, latitudeEnd);
				System.out.println(list[i].getAbsolutePath());
			}
		}
	}
	
	public void findGpsFileByLB(String rootDirectory, double longtitudeStart, double longtitudeEnd, double latitudeStart, double latitudeEnd){
		File[] list = (new File(rootDirectory)).listFiles();
		for(int i=0; i<list.length; i++){
			if(list[i].isDirectory()){
				findGpsFile(list[i].getAbsolutePath());
			}else{
				this.analyzeGpsCsv(list[i]);
				System.out.println(list[i].getAbsolutePath());
			}
		}
	}
	
	public void findGpsFile(String rootDirectory){
		File[] list = (new File(rootDirectory)).listFiles();
		for(int i=0; i<list.length; i++){
			if(list[i].isDirectory()){
				findGpsFile(list[i].getAbsolutePath());
			}else{
				this.analyzeGpsCsv(list[i]);
				System.out.println(list[i].getAbsolutePath());
			}
		}
	}
	
	public static String testPost(String x, String y) throws IOException {

		URL url = new URL("http://api.map.baidu.com/geocoder?ak="+ ConfParser.ak
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
			byte[] result = null;
			String resultStr = "";
			try {
				result = ssq.getBytes("UTF-8");
				resultStr = new String(result,"GBK");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return ssq;
		}
		return null;
	}
}
