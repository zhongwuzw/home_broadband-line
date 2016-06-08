package com.opencassandra.descfile;

import java.io.File;

public class ConfParser {
	static String paraPath = "res/conf/monitor.ini";
	public static String[] srcReportPath = null;
	public static String[] table = null;
	public static String[] mapLevel = null;
	public static String org_prefix = "/home/nfs/ctpOTS";
	public static String csvwritepath = "/home/user/liugang/OTS/result";
	public static String srcPath = "/OTS/";
	public static String backReportPath = "/OTS/";
	public static String destRootPrefix4Database = "/OTS/";
	public static String url = "jdbc:mysql://192.168.85.233:3306/testdataanalyse";
	public static String user = "CM9VDa==";
	public static String password = "y21YAwn0CdiZmW==";
	//public static String user = "root";
	//public static String password = "cmrictp233";
	public static String ak = "2b866a6daac9014292432d81fe9b47e3";
	public static double longtitudeStart = 116.1;
	public static double longtitudeEnd = 116.7;
	public static double latitudeStart = 39.7;
	public static double latitudeEnd = 40.2;
	public static double LATinterval = 0.5;
	public static double LNGinterval = 0.5;
	public static String dataStart = "201401";
//	public static String apikey = "97";
	public static String code[] = null;
	public static String getCodePath = "http://192.168.16.72/ctp/orginfo/v3/query/getSecondOrgsByCode.do";
	public static String[] appname = null;
	public static String[] tableType = null;
	public static String writeCsvOpen = "close";
	public static String areaDataOpen = "close";
	//
	public static String pathType = "default";
	//配置的时间标志
	public static String[] testdate = null;
	//终端报告解析时配置的中间表名
	public static  String temptable ="gridtable";
	//终端报告解析时配置的须解析的文件夹名（http,ping）
	public static String[]  folder = null;
	//网络2G,3G,4G的配置文件，网格化信号强度
	public static String[] nettype = null;
	//两个数据库ip配置的
	public static String dataurl = "";
	//数据的类型（ping,http_test）确定数据该入哪一个表
	public static String[] datatype=null; 
	//数据网格化数据源ip数据库
	public static String srcurl="jdbc:mysql://192.168.16.96:5050/testdataanalyse_static";
	//数据网格化数据源数据库用户名
	public static String srcuser="z2jHC2u=";
	//数据网格化数据源数据库密码
	public static String srcpassword="z2jHC2uYmdeXmduZmq==";
	//数据网格化后数据入库ip数据库
	public static String dsturl="jdbc:mysql://192.168.16.96:5050/servicequalitymap_static";
	//数据网格化数据入库数据库用户名
	public static String dstuser="z2jHC2u=";
	//数据网格化数据入库数据库密码
	public static String dstpassword="z2jHC2uYmdeXmduZmq==";
	//网格化数据时对应的表标识及对应字段
	public static String[] pointertype = null;
	public static String[] pointer = null;
	//百度地图开关
	public static String baiduSwitch = "";
	//处理区域图时的数据源数据库
	public static String basename = "";
	public static String issubmeter="";
	public static String[] areachart=null;
	public static String[] interval = null;
	public static String subdatabase = "";
	public static String time="";
	public static String base_prefix = "";
	
	
	public static String caseinfo="";
	
	static{
		String mystr = System.getProperty("user.dir");;
		File myFile = new File(mystr+System.getProperty("file.separator")+ConfParser.paraPath);
		if(myFile.exists()){
			org_prefix = IniReader.getValue("MonitorPath", "org_prefix", myFile.getAbsolutePath());
			if(org_prefix!=null && !org_prefix.equals("")){
				if(!org_prefix.endsWith("\\")&&!org_prefix.endsWith("/")){
					org_prefix += System.getProperty("file.separator");
				}
			}
			String srcReportPathStr = IniReader.getValue("MonitorPath", "srcreportpath", myFile.getAbsolutePath());
			if(srcReportPathStr!=null && srcReportPathStr.contains(",")){
				srcReportPath = srcReportPathStr.split(",");
				for(int i=0; i<srcReportPath.length; i++){
					if(!srcReportPath[i].endsWith("\\")&&!srcReportPath[i].endsWith("/")&&!srcReportPath[i].isEmpty()){
						srcReportPath[i] += System.getProperty("file.separator");
					}
				}
			}else{
				srcReportPath = new String[]{srcReportPathStr};
			}
			
			String mapLevelStr = IniReader.getValue("MonitorPath", "maplevel", myFile.getAbsolutePath());
			if(mapLevelStr!=null && mapLevelStr.contains(",")){
				mapLevel = mapLevelStr.split(",");
			}else{
				mapLevel = new String[]{mapLevelStr};
			}
			
			String tableStr = IniReader.getValue("MonitorPath", "table", myFile.getAbsolutePath());
			if(tableStr!=null && tableStr.contains(",")){
				table = tableStr.split(",");
			}else{
				table = new String[]{tableStr};
			}
			
			String appnameStr = IniReader.getValue("MonitorPath", "appname", myFile.getAbsolutePath());
			if(appnameStr!=null && appnameStr.contains(",")){
				appname = appnameStr.split(",");
			}else{
				appname = new String[]{appnameStr};
			}
			String tableTypeStr = IniReader.getValue("MonitorPath", "tableType", myFile.getAbsolutePath());
			if(tableTypeStr!=null && tableTypeStr.contains(",")){
				tableType = tableTypeStr.split(",");
			}else{
				tableType = new String[]{tableTypeStr};
			}
		/*	if(tableType==null || tableType.length == 0 || (tableType.length ==1 && tableType[1].isEmpty())){
				tableType = new String[]{"http","ping","speed_test"};
			}
			*/
			ak = IniReader.getValue("MonitorPath", "ak", myFile.getAbsolutePath());
			if(ak==null){
				ak = "";
			}
			
//			apikey = IniReader.getValue("MonitorPath", "apikey", myFile.getAbsolutePath());
//			if(apikey==null){
//				apikey = "";
//			}
			
			String codeStr = IniReader.getValue("MonitorPath", "code", myFile.getAbsolutePath());
			if(codeStr!=null && codeStr.contains(",")){
				code = codeStr.split(",");
			}else{
				code = new String[]{codeStr};
			}
			
			
			
			getCodePath = IniReader.getValue("MonitorPath", "getCodePath", myFile.getAbsolutePath());
			if(getCodePath==null){
				getCodePath = "";
			}
			
			url = IniReader.getValue("MonitorPath", "url", myFile.getAbsolutePath());
			if(url==null){
				url = "";
			}
			dataurl = IniReader.getValue("MonitorPath", "dataurl", myFile.getAbsolutePath());
			if(dataurl==null){
				dataurl = "";
			}
			user = IniReader.getValue("MonitorPath", "user", myFile.getAbsolutePath());
			
			if(user==null){
				user = "";
			}else{
				user = Base64Decode.getStringToUpOrLow(user);
			}
			System.out.println(user+"  :::::");
			password = IniReader.getValue("MonitorPath", "password", myFile.getAbsolutePath());
			
			if(password==null){
				password = "";
			}else{
				password = Base64Decode.getStringToUpOrLow(password);
			}
			System.out.println(password+"  ::::");
			backReportPath = IniReader.getValue("MonitorPath", "backreportpath", myFile.getAbsolutePath());
			if(backReportPath!=null && !backReportPath.equals("")){
				if(!backReportPath.endsWith("\\")&&!backReportPath.endsWith("/")){
					backReportPath += System.getProperty("file.separator");
				}
			}
			csvwritepath = IniReader.getValue("MonitorPath", "csvwritepath", myFile.getAbsolutePath());
			if(csvwritepath!=null && !csvwritepath.equals("")){
				if(!csvwritepath.endsWith("\\")&&!csvwritepath.endsWith("/")){
					csvwritepath += System.getProperty("file.separator");
				}
			}
			srcPath = IniReader.getValue("MonitorPath", "srcpath", myFile.getAbsolutePath());
			if(srcPath!=null && !srcPath.equals("")){
				if(!srcPath.endsWith("\\")&&!srcPath.endsWith("/")){
					srcPath += System.getProperty("file.separator");
				}
			}
			destRootPrefix4Database = IniReader.getValue("MonitorPath", "destrootprefix4database", myFile.getAbsolutePath());
			if(destRootPrefix4Database!=null && !destRootPrefix4Database.equals("")){
				if(!destRootPrefix4Database.endsWith("\\")&&!destRootPrefix4Database.endsWith("/")){
					destRootPrefix4Database += System.getProperty("file.separator");
				}
			}
			
			String longtitudeStartStr = IniReader.getValue("MonitorPath", "longtitudestart", myFile.getAbsolutePath());
			if(longtitudeStartStr!=null && !longtitudeStartStr.equals("")){
				try{
					longtitudeStart = Double.parseDouble(longtitudeStartStr);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("The String is --->"+ longtitudeStartStr);
				}
			}
			
			String longtitudeEndStr = IniReader.getValue("MonitorPath", "longtitudeend", myFile.getAbsolutePath());
			if(longtitudeEndStr!=null && !longtitudeEndStr.equals("")){
				try{
					longtitudeEnd = Double.parseDouble(longtitudeEndStr);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("The String is --->"+ longtitudeEndStr);
				}
			}
			
			String latitudeStartStr = IniReader.getValue("MonitorPath", "latitudestart", myFile.getAbsolutePath());
			if(latitudeStartStr!=null && !latitudeStartStr.equals("")){
				try{
					latitudeStart = Double.parseDouble(latitudeStartStr);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("The String is --->"+ latitudeStartStr);
				}
			}
			
			String latitudeEndStr = IniReader.getValue("MonitorPath", "latitudeend", myFile.getAbsolutePath());
			if(latitudeEndStr!=null && !latitudeEndStr.equals("")){
				try{
					latitudeEnd = Double.parseDouble(latitudeEndStr);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("The String is --->"+ latitudeEndStr);
				}
			}
			String LATintervalStr = IniReader.getValue("MonitorPath", "LATinterval", myFile.getAbsolutePath());
			if(LATintervalStr!=null && !LATintervalStr.equals("")){
				try{
					LATinterval = Double.parseDouble(LATintervalStr);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("The String is --->"+ LATintervalStr);
				}
			}
			String LNGintervalStr = IniReader.getValue("MonitorPath", "LNGinterval", myFile.getAbsolutePath());
			if(LNGintervalStr!=null && !LNGintervalStr.equals("")){
				try{
					LNGinterval = Double.parseDouble(LNGintervalStr);
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("The String is --->"+ LNGintervalStr);
				}
			}
			
			dataStart = IniReader.getValue("MonitorPath", "dataStart", myFile.getAbsolutePath());
			if(dataStart==null){
				dataStart = "201401";
			}
			
			writeCsvOpen = IniReader.getValue("MonitorPath", "writeCsvOpen", myFile.getAbsolutePath());
			if(writeCsvOpen==null){
				writeCsvOpen = "";
			}
			
			areaDataOpen = IniReader.getValue("MonitorPath", "areaDataOpen", myFile.getAbsolutePath());
			if(areaDataOpen==null){
				areaDataOpen = "";
			}
			pathType = IniReader.getValue("MonitorPath", "pathType", myFile.getAbsolutePath());
			if(pathType==null || pathType.isEmpty()){
				pathType = "default";
			}
			
			String dates = IniReader.getValue("MonitorPath", "testdate", myFile.getAbsolutePath());
			
			if(dates!=null && dates.contains(",")){
				testdate = dates.split(",");
			}else{
				testdate = new String[]{dates};
			}
			System.out.println(dates+"   :::::::");
			temptable = IniReader.getValue("MonitorPath", "temptable", myFile.getAbsolutePath());
			if(temptable==null || temptable.isEmpty()){
				temptable = "gridtable";
			}
			
			String  folders = IniReader.getValue("MonitorPath", "folder", myFile.getAbsolutePath());
			
			if( folders!=null && folders.contains(",")){
				 folder = folders.split(",");
			}else{
				 folder = new String[]{folders};
			}
			String  nettypes = IniReader.getValue("MonitorPath", "nettype", myFile.getAbsolutePath());
			
			if( nettypes!=null && nettypes.contains(",")){
				nettype = nettypes.split(",");
			}else{
				nettype = new String[]{nettypes};
			}
			String  datatypes = IniReader.getValue("MonitorPath", "datatype", myFile.getAbsolutePath());
			
			if( datatypes!=null && datatypes.contains(",")){
				datatype = datatypes.split(",");
			}else{
				datatype = new String[]{datatypes};
			}
			//数据网格化数据源ip
			srcurl = IniReader.getValue("MonitorPath", "srcurl", myFile.getAbsolutePath());
			if(srcurl==null || srcurl.isEmpty()){
				srcurl = "";
			}
			srcuser = IniReader.getValue("MonitorPath", "srcuser", myFile.getAbsolutePath());
			
			if(srcuser==null){
				srcuser = "";
			}else{
				srcuser = Base64Decode.getStringToUpOrLow(srcuser);
			}
			srcpassword = IniReader.getValue("MonitorPath", "srcpassword", myFile.getAbsolutePath());
			
			if(srcpassword==null){
				srcpassword = "";
			}else{
				srcpassword = Base64Decode.getStringToUpOrLow(srcpassword);
			}
			//网格化后数据入库ip用户名及密码
			dsturl = IniReader.getValue("MonitorPath", "dsturl", myFile.getAbsolutePath());
			if(dsturl==null || dsturl.isEmpty()){
				dsturl = "";
			}
			dstuser = IniReader.getValue("MonitorPath", "dstuser", myFile.getAbsolutePath());
			
			if(dstuser==null){
				dstuser = "";
			}else{
				dstuser = Base64Decode.getStringToUpOrLow(dstuser);
			}
			dstpassword = IniReader.getValue("MonitorPath", "dstpassword", myFile.getAbsolutePath());
			
			if(dstpassword==null){
				dstpassword = "";
			}else{
				dstpassword = Base64Decode.getStringToUpOrLow(dstpassword);
			}
			//数据网格化对应的表标识及对应字段
			String pointertypes = IniReader.getValue("MonitorPath", "pointertype", myFile.getAbsolutePath());
			if(pointertypes!=null && pointertypes.contains(",")){
				pointertype = pointertypes.split(",");
			}else{
				pointertype = new String[]{pointertypes};
			}
			//
			String pointers = IniReader.getValue("MonitorPath", "pointer", myFile.getAbsolutePath());
			if(pointers!=null && pointers.contains(",")){
				pointer = pointers.split(",");
			}else{
				pointer = new String[]{pointers};
			}
			
			String intervals = IniReader.getValue("MonitorPath", "interval", myFile.getAbsolutePath());
			if(intervals!=null && intervals.contains(",")){
				interval = intervals.split(",");
			}else{
				interval = new String[]{intervals};
			}
			
			baiduSwitch = IniReader.getValue("MonitorPath", "baiduSwitch", myFile.getAbsolutePath());
			if(baiduSwitch==null || baiduSwitch.isEmpty()){
				baiduSwitch = "offplat";
			}
			//默认数据名数据库
			subdatabase = IniReader.getValue("MonitorPath", "subdatabase", myFile.getAbsolutePath());

			//处理区域图时的数据源数据库名
			basename = IniReader.getValue("MonitorPath", "basename", myFile.getAbsolutePath());
			issubmeter = IniReader.getValue("MonitorPath", "issubmeter", myFile.getAbsolutePath());
			
			//终端分布、时段分布数据区域标识
			String areacharts = IniReader.getValue("MonitorPath", "areachart", myFile.getAbsolutePath());
			if(areacharts!=null && areacharts.contains(",")){
				areachart = areacharts.split(",");
			}else{
				areachart = new String[]{areacharts};
			}
			
			time = IniReader.getValue("MonitorPath", "time", myFile.getAbsolutePath());

			base_prefix = IniReader.getValue("MonitorPath", "base_prefix", myFile.getAbsolutePath());
			
			caseinfo = IniReader.getValue("MonitorPath", "caseinfo", myFile.getAbsolutePath());
		}
		
	}
	public ConfParser(){
		
	}
	public static void main(String[] args){
		//System.out.println(ConfParser.srcPath);
	}
}
