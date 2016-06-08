package com.opencassandra.v01.dao.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.ResultSet.Flag;
import org.apache.commons.io.output.FileWriterWithEncoding;

import com.csvreader.CsvWriter;
import com.opencassandra.descfile.ConfParser;
import com.sun.corba.se.spi.orb.StringPair;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class InsertCSVByDataToGPS {
	
	private File csvFile = null;
	private CsvWriter cwriter = null;
	private BufferedWriter writer = null;
	private double LNGinterval = 0.001;
	private double LATinterval = 0.0008;

	public void insertCsvGPSData(Map dataMap,String numType,String deviceOrg,String detailreport){
		if(dataMap!=null){
			if(numType.equals("03001")){
				insertCsvGPSDataHttp(dataMap, numType, deviceOrg, detailreport);
			}else if(numType.equals("04002")){
				insertCsvGPSDataPing(dataMap, numType, deviceOrg, detailreport);
			}else if(numType.equals("01001")){
				insertCsvGPSDataSpeedTest(dataMap, numType, deviceOrg, detailreport);
			}else{
				System.out.println("文件类型错误");
			}
		}
	}
	
	private void insertCsvGPSDataSpeedTest(Map dataMap, String numType,
			String deviceOrg, String detailreport) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String lac_tac = "";
		String cid_pci = "";
		String gpsStr = "";
		String latitude = "";
		String longitude = "";
		String model = "";//终端类型
		String logversion = "";//软件版本
		String device_org = deviceOrg;//终端分组
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		map.put("appid", appid);
		map.put("device_org", device_org);
		map.put("time", "`"+time);
		
		String location = "";//测试地点
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String max_up = "";//最大上行速率
		String max_down = "";//最大下行速率
		String avg_up = "";//上行平均速率
		String avg_down = "";//下行平均速率
		String protocol = "";//测试类型
		String downlink = "";//下行线程数
		String uplink = "";//上行线程数
		String delay = "";//时延
		String server_ip = "";//服务器信息
		String net_type = "";//网络类型
		String signal_strength = "";//信号迁都
		String sinr = "";//SINR
		
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
		String spaceGpsStr = "";//测试地点
		//获取GPS位置信息----start
		if(dataMap.containsKey("GPS信息")){
			gpsStr = (String)(dataMap.get("GPS信息")==null?"":dataMap.get("GPS信息"));
		}else if(dataMap.containsKey("GPS位置信息")){
			gpsStr = (String)(dataMap.get("GPS位置信息")==null?"":dataMap.get("GPS位置信息"));
		}else if(dataMap.containsKey("GPS位置")){
			gpsStr = (String)(dataMap.get("GPS位置")==null?"":dataMap.get("GPS位置"));
		}else if(dataMap.containsKey("测试位置")){
			gpsStr = (String)(dataMap.get("测试位置")==null?"":dataMap.get("测试位置"));
		}else if(dataMap.containsKey("测试GPS位置")){
			gpsStr = (String)(dataMap.get("测试GPS位置")==null?"":dataMap.get("测试GPS位置"));
		}else{
			gpsStr = "";
		}
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("上行速率".equals(key)||"上行速率(Mbps)".equals(key) || "Up Speed".equals(key) || "Up Speed(Mbps)".equals(key) || "Up Speed(Kbps)".equals(key) || "Upload Speed".equals(key) || "Upload Speed(Mbps)".equals(key)){
				avg_up = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("下行速率".equals(key)||"下行速率(Mbps)".equals(key)  || "Down Speed".equals(key) || "Down Speed(Mbps)".equals(key) || "Down Speed(Kbps)".equals(key) || "Download Speed".equals(key) || "Download Speed(Mbps)".equals(key)){
				avg_down = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("maximum up".equals(key.toLowerCase()) || "maximum down".equals(key.toLowerCase()) || "上行最大速率(Mbps)".equals(key)){
				max_up = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("maximum down".equals(key.toLowerCase()) || "maximum down(mbps)".equals(key.toLowerCase()) || "下行最大速率(Mbps)".equals(key)){
				max_down = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("时延".equals(key) || "时延(ms)".equals(key) || "平均时延(ms)".equals(key) || "平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase()) || "latency".equals(key.toLowerCase()) || "latency(ms)".equals(key.toLowerCase())){
				delay = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("服务器信息".equals(key) || "服务器地址".equals(key) || "Server Info".equals(key) || "Service Info".equals(key)){
				server_ip = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测速类型".equals(key) || "测试类型".equals(key) || "Protocol".equals(key)){
				protocol = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("上行线程数".equals(key) || "Uplink Thread(s)".equals(key) || "Uplink Thread Count".equals(key) || "Uplink Thread Counts".equals(key)){
				uplink = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("下行线程数".equals(key) || "Downlink Thread(s)".equals(key) || "Downlink Thread Count".equals(key) || "Downlink Thread Counts".equals(key) ){
				downlink = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("avg_up",avg_up);
		map.put("avg_down",avg_down);
		map.put("max_up",max_up);
		map.put("max_down",max_down);
		map.put("delay",delay);
		map.put("server_ip",server_ip);
		map.put("protocol",protocol);
		map.put("uplink",uplink.isEmpty()?0:uplink);
		map.put("downlink",downlink.isEmpty()?0:downlink);
		
		if(max_down.equals("") && !avg_down.isEmpty()){
			max_down = avg_down;
		}
		if(max_up.equals("") && !avg_up.isEmpty()){
			max_up = avg_up;
		}
		
		map.put("max_down", max_down);
		map.put("max_up", max_up);
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		boolean flag = writeCsv(map,numType,longitudeNum,latitudeNum);
		System.out.println("写入csv数据："+flag);
	}

	private void insertCsvGPSDataPing(Map dataMap, String numType,
			String deviceOrg, String detailreport) {
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		String appid = dataMap.get("appid")==null?"":dataMap.get("appid").toString();//appid
		
		map.put("appid", appid);
		map.put("detailreport", detailreport);
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));

		String device_org = deviceOrg;//终端分组
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = dataMap.get("time")==null?"":dataMap.get("time").toString();//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		
		map.put("device_org", device_org);
		map.put("time", "`"+time);
		
		String domain_address = "";
		String max_rtt = "";
		String min_rtt = "";
		String avg_rtt = "";
		String times = "";
		String success_rate = "";//成功率
		String packet_loss_rate = "";//丢包率
		
		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
		String spaceGpsStr = "";//测试地点
		//获取GPS位置信息----start
		if(dataMap.containsKey("GPS信息")){
			gpsStr = (String)(dataMap.get("GPS信息")==null?"":dataMap.get("GPS信息"));
		}else if(dataMap.containsKey("GPS位置信息")){
			gpsStr = (String)(dataMap.get("GPS位置信息")==null?"":dataMap.get("GPS位置信息"));
		}else if(dataMap.containsKey("GPS位置")){
			gpsStr = (String)(dataMap.get("GPS位置")==null?"":dataMap.get("GPS位置"));
		}else if(dataMap.containsKey("测试位置")){
			gpsStr = (String)(dataMap.get("测试位置")==null?"":dataMap.get("测试位置"));
		}else if(dataMap.containsKey("测试GPS位置")){
			gpsStr = (String)(dataMap.get("测试GPS位置")==null?"":dataMap.get("测试GPS位置"));
		}else{
			gpsStr = "";
		}
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		if(longitudeNum == 0.0 || longitudeNum == 0){
			map.put("longitude","");
		}
		if(latitudeNum == 0.0 || latitudeNum == 0){
			map.put("latitude","");
		}
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("地址".equals(key) || "Address".equals(key) || "address".equals(key)){
				domain_address = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("平均时延(ms)".equals(key) || "平均时延".equals(key) || "average delay(ms)".equals(key.toLowerCase()) || "avg latency(ms)".equals(key.toLowerCase()) || "avg time(ms)".equals(key.toLowerCase()) || "avg latency".equals(key.toLowerCase()) || "delay".equals(key.toLowerCase()) || "delay(ms)".equals(key.toLowerCase())){
				avg_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("the minimum delay(ms)".equals(key.trim().toLowerCase()) ||"最大时延(ms)".equals(key)||"最大时延".equals(key) || "max latency(ms)".equals(key.toLowerCase()) || "max latency".equals(key.toLowerCase())){
				max_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("the maximum delay(ms)".equals(key.trim().toLowerCase()) || "最小时延(ms)".equals(key)||"最小时延".equals(key) || "min latency(ms)".equals(key.toLowerCase()) || "min latency".equals(key.toCharArray())){
				min_rtt = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试次数".equals(key)||"重复次数".equals(key) || "repetition".equals(key.toLowerCase()) || "number of repetitions".equals(key.toLowerCase()) || "Test Counts".equals(key)){
				times = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("packet_loss_rate".equals(key.toLowerCase()) || "丢包率".equals(key)){
				packet_loss_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		map.put("logversion", logversion);
		map.put("model",model);
		map.put("domain_address",domain_address);
		map.put("avg_rtt",avg_rtt);
		map.put("max_rtt",max_rtt);
		map.put("min_rtt",min_rtt);
		map.put("times",times.isEmpty()?0:times);
		map.put("packet_loss_rate",packet_loss_rate);
		map.put("success_rate", success_rate);
		
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(lac1 != null){
				lac_tac = lac1;
			}
			if(cid1 != null){
				cid_pci = cid1;
			}
			if(lac2 != null){
				lac_tac = lac2;
			}
			if(cid2 != null){
				cid_pci = cid2;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		boolean flag = writeCsv(map,numType,longitudeNum,latitudeNum);
		System.out.println("写入csv数据："+flag);
	}

	public void insertCsvGPSDataHttp(Map dataMap,String numType,String deviceOrg,String detailreport){
		List nameList = new ArrayList();
		List valueList = new ArrayList();
		Map map = new HashMap();
		Set keySet = dataMap.keySet();
		Iterator iter = keySet.iterator();
		String imei = (String)(dataMap.get("imei")==null?"":dataMap.get("imei"));
		String file_path = (String)(dataMap.get("filePath")==null?"":dataMap.get("filePath"));
		String detailreportvalue = (String)(dataMap.get("detailreport")==null?"":dataMap.get("detailreport"));
		map.put("detailreport", detailreport);
		
		map.put("imei",imei);
		map.put("file_path",file_path.replace("\\", "|").replace("//", "|"));
		
		String device_org = deviceOrg;//终端分组
		String longitude = "";
		String latitude = "";
		String province = "";//测试省
		String district = "";//测试district
		String city = "";//测试city
		String time = (String)(dataMap.get("time")==null?"":dataMap.get("time"));;//测试时间
		String model = "";//终端类型
		String logversion = "";//软件版本
		map.put("device_org", device_org);
		map.put("time", "`"+time);
		
		String url = "";//文件地址
		String testfileurl = "";//测试文件地址
		String resource_size = "";//文件大小
		String duration = "";//下载/上传时长
		String avg_rate = "";//平均速率
		String max_rate = "";//最大速率
		String avg_latency = "";//平均时长
		String file_type = "";//测试类型
		String success_rate = "";//成功率

		String location = "";//测试地点
		String gpsStr = "";
		String net_type = "";//网络类型
		String signal_strength = "";//信号强度
		String sinr = "";//SINR
		String lac = "";
		String cid = "";
		String tac = "";
		String pci = "";
		String lac_tac = "";
		String cid_pci = "";
		String net_type1 = "";
		String signal_strength1 = "";
		String sinr1 = "";
		String cell1 = "";
		String net_type2 = "";
		String signal_strength2 = "";
		String sinr2 = "";
		String cell2 = "";
		String wifi_ss = "";//WIFI SIGNAL STRENGTH
		
		boolean flag1 = false;
		boolean flag2 = false;
		
		String spaceGpsStr = "";//测试地点
		//获取GPS位置信息----start
		if(dataMap.containsKey("GPS信息")){
			gpsStr = (String)(dataMap.get("GPS信息")==null?"":dataMap.get("GPS信息"));
		}else if(dataMap.containsKey("GPS位置信息")){
			gpsStr = (String)(dataMap.get("GPS位置信息")==null?"":dataMap.get("GPS位置信息"));
		}else if(dataMap.containsKey("GPS位置")){
			gpsStr = (String)(dataMap.get("GPS位置")==null?"":dataMap.get("GPS位置"));
		}else if(dataMap.containsKey("测试位置")){
			gpsStr = (String)(dataMap.get("测试位置")==null?"":dataMap.get("测试位置"));
		}else if(dataMap.containsKey("测试GPS位置")){
			gpsStr = (String)(dataMap.get("测试GPS位置")==null?"":dataMap.get("测试GPS位置"));
		}else{
			gpsStr = "";
		}
		map.put("gps", gpsStr);
		if(gpsStr.contains(" ")){
			String[] gps = transGpsPoint(gpsStr);
			if(gps!=null && gps[0]!=null && gps[1]!=null){
				longitude = gps[0];
				latitude = gps[1];
			}
		}
		double longitudeNum = 0.0;
		double latitudeNum = 0.0;
		if(longitude.isEmpty() && latitude.isEmpty()){
			
		}else{
			double[] em = Earth2Mars.transform(Double.parseDouble(longitude), Double.parseDouble(latitude));
			double[] bf = BdFix.bd_encrypt(em[0], em[1]);
			longitudeNum = bf[0];
			latitudeNum = bf[1];
		}
		map.put("longitude",longitudeNum);
		map.put("latitude",latitudeNum);
		
		longitudeNum = ((int) (longitudeNum / LNGinterval)) * LNGinterval + LNGinterval / 2;
		latitudeNum = ((int) (latitudeNum / LATinterval)) * LATinterval + LATinterval / 2;
		
		//获取GPS位置信息----end
		if(dataMap.containsKey("网络(1)网络制式") || dataMap.containsKey("Network(1) Type") || dataMap.containsKey("Network (1) standard")){
			flag1 = true;
		}
		if(dataMap.containsKey("网络(2)网络制式") || dataMap.containsKey("Network(2) Type") || dataMap.containsKey("Network (1) standard")){
			flag2 = true;
		}
		while (iter.hasNext()) {
			String key = iter.next() + "";
			if (key == null || key.isEmpty()) {
				continue;
			}
			if("平均速率(Kbps)".equals(key) || "平均速率(Mbps)".equals(key) ||"平均速率（Mbps）".equals(key) || "average speed (mbps)".equals(key.toLowerCase()) || "average speed (kbps)".equals(key.toLowerCase()) || "avg speed(kbps)".equals(key.toLowerCase()) || "avg speed(mbps)".equals(key.toLowerCase())){
				avg_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("Max Speed(Kbps)".equals(key) || "Max Speed(Mbps)".equals(key) || "最大速率(Mbps)".equals(key) || "最大速率(Kbps)".equals(key) || "最大速率（Mbps）".equals(key) || "最大速率（Kbps）".equals(key)){
				max_rate = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("平均时长(ms)".equals(key) || "平均时延".equals(key) || "平均时延(ms)".equals(key) || "Avg Latency(ms)".equals(key) || "Avg Time(ms)".equals(key) || "Latency".equals(key) || "Delay".equals(key) || "Delay(ms)".equals(key)){
				avg_latency = (String)(dataMap.get(key)==null?"":dataMap.get(key));
			}else if("logversion".equals(key.toLowerCase()) || "日志版本".equals(key)){
				logversion = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("信号强度".equals(key) || "Signal Strength".equals(key)){
				signal_strength = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("终端类型".equals(key) || "终端型号".equals(key) || "device type".equals(key.toLowerCase()) || "terminal model".equals(key.toLowerCase()) || "terminal type".equals(key.toLowerCase()) || "terminal mode".equals(key.toLowerCase()) || "device model".equals(key.toLowerCase()) || "terminal models".equals(key.toLowerCase())){
				model = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("类型".equals(key) || "Type".equals(key) || "Test Type".equals(key) || "测试类型".equals(key)){
				file_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("文件".equals(key) || "File".equals(key)){
				url = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("测试文件".equals(key) || "Test File".equals(key)){
				testfileurl = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("文件大小".equals(key) || "File Size".equals(key) || "File Size(MB)".equals(key) || "文件大小(MB)".equals(key)){
				resource_size = (String) (dataMap.get(key)==null?"":dataMap.get(key));
//				if(resource_size.equals("") || resource_size.equals("--") || !resource_size.contains(".")){
				
//				}else
//				{
//					map.put("resource_size",resource_size.substring(resource_size.lastIndexOf("/")+1, resource_size.lastIndexOf(".")));
//				}
			}else if("成功率".equals(key) || "success rate".equals(key.toLowerCase())){
				success_rate = (String)(dataMap.get(key)==null?"":dataMap.get(key));
				
			}else if("时长(s)".equals(key) || "时长（s）".equals(key) || "时长(ms)".equals(key) || "duration".equals(key.toLowerCase()) || "duration(ms)".equals(key.toLowerCase())){
				duration = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				
			}else if("LAC".equals(key)){
				String lacStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(lacStr.indexOf("=")!=-1){
					lac = lacStr.substring(lacStr.indexOf("=")+1,lacStr.length());
				}else{
					lac = lacStr;
				}
			}else if("CID".equals(key)){
				String cidStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(cidStr.indexOf("=")!=-1){
					cid = cidStr.substring(cidStr.indexOf("=")+1,cidStr.length());
				}else{
					cid = cidStr;
				}
			}else if("LAC/CID".equals(key)){
				String lacCid = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(lacCid.indexOf("/")!=-1 || lacCid.indexOf("N/A")!=-1){
					String[] cellInfo = lacCid.split("/");
					lac = cellInfo[0];
					cid = cellInfo[1];
				}
			}else if("TAC".equals(key)){
				String tacStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(tacStr.indexOf("=")!=-1){
					tac = tacStr.substring(tacStr.indexOf("=")+1,tacStr.length()-1);
				}else{
					tac = tacStr;
				}
			}else if("PCI".equals(key)){
				String pciStr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(pciStr.indexOf("=")!=-1){
					pci = pciStr.substring(pciStr.indexOf("=")+1,pciStr.length()-1);
				}else{
					pci = pciStr;
				}
			}else if("TAC/PCI".equals(key)){
				String tacPci = (String) (dataMap.get(key)==null?"":dataMap.get(key));
				if(tacPci.equals("N/A/N/A")){
					tac = "N/A";
					pci = "N/A";
				}else{
					if(tacPci.indexOf("/")!=-1 || tacPci.indexOf("N/A")!=-1){
						String[] cellInfo = tacPci.split("/");
						tac = cellInfo[0];
						pci = cellInfo[1];
					}
				}
			}else if("网络类型".equals(key) || "网络制式".equals(key) || "network type".equals(key.toLowerCase())){
				net_type = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("SINR".equals(key)){
				sinr = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)网络制式".equals(key) || "network(1) type".equals(key.toLowerCase()) || "network (1) standard".equals(key.toLowerCase())){
				net_type1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)信号强度".equals(key) || "网络(1)信号强度(dBm)".equals(key) || "network(1) signal strength".equals(key.toLowerCase()) || "network(1) signal strength(dbm)".equals(key.toLowerCase())){
				signal_strength1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)小区信息".equals(key) || "network(1) cell".equals(key.toLowerCase()) || "network(1)".equals(key.toLowerCase())){
				cell1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(1)SINR".equals(key) || "网络(1)SINR(dB)".equals(key) || "network(1) sinr".equals(key.toLowerCase()) || "network(1) sinr(db)".equals(key.toLowerCase())){
				sinr1 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)网络制式".equals(key) || "network(2) type".equals(key.toLowerCase()) || "network (2) standard".equals(key.toLowerCase())){
				net_type2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)信号强度".equals(key) || "网络(2)信号强度(dBm)".equals(key)	 || "network(2) signal strength".equals(key.toLowerCase())){
				signal_strength2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)小区信息".equals(key) || "network(2) cell".equals(key.toLowerCase()) || "network(2)".equals(key.toLowerCase())){
				cell2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("网络(2)SINR".equals(key) || "网络(2)SINR(dB)".equals(key) || "network(2) sinr".equals(key.toLowerCase()) || "network(2) sinr(db)".equals(key.toLowerCase())){
				sinr2 = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}else if("WIFI信号强度".equals(key) || "WIFI信号强度(dBm)".equals(key) || "wifi signal strength".equals(key.trim().toLowerCase()) || "wifi signal strength(dbm)".equals(key.trim().toLowerCase())){
				wifi_ss = (String) (dataMap.get(key)==null?"":dataMap.get(key));
			}
		}
		try {
			double avg_num = Double.parseDouble(avg_rate);
			if(avg_num>100 || avg_num<0.1){
				System.out.println("平均速率错误 不进行插入操作");
				return ;
			}else{
				map.put("avg_rate", avg_rate);	
			}
		} catch (Exception e) {
			System.out.println("avg_rate 不为纯数字");
			return ;
		}
		map.put("max_rate", max_rate);
		map.put("avg_latency",avg_latency);
		map.put("logversion", logversion);
		map.put("signal_strength", signal_strength);
		map.put("model",model);
		map.put("file_type", file_type);
		map.put("url", url);
		map.put("resource_size",resource_size);
		map.put("success_rate", success_rate);
		map.put("duration",duration);
		//处理上传类型时url取文件中的值
		if(file_type.equals("上传") || file_type.toLowerCase().equals("upload")){
			map.put("url", url);
		}else{
			if(!testfileurl.isEmpty()){
				map.put("url", url);
			}
			if(!url.isEmpty()){
				map.put("url", url);
			}
		}
		String lac1 = "";
		String cid1 = "";
		if(cell1.indexOf("/")!=-1 || cell1.indexOf("N/A")!=-1){
			String[] cellInfo = cell1.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac1 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid1 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac1 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid1 = cellInfo[1]+"|"+pci;
			}
		}
		String lac2 = "";
		String cid2 = "";
		if(cell2.indexOf("/")!=-1 || cell2.indexOf("N/A")!=-1){
			String[] cellInfo = cell2.split("/");
			if(cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0];
			}
			if(tac!=null && !tac.equals("")){
				lac2 = tac;
			}
			if(cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1];
			}
			if(pci!=null && !pci.equals("")){
				cid2 = pci;
			}
			if(tac!=null && !tac.equals("") && cellInfo[0]!=null && !cellInfo[0].equals("")){
				lac2 = cellInfo[0]+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cellInfo[1]!=null && !cellInfo[1].equals("")){
				cid2 = cellInfo[1]+"|"+pci;
			}
		}
		//网络(1)信息存在时 当前数据返回的是网络(1)的信息
		if(flag1 && net_type1.equals(net_type)){
			sinr = sinr1;
			net_type = net_type1;
			signal_strength = signal_strength1;
			lac_tac = lac1;
			cid_pci = cid1;
		}
		//网络(2)信息存在时 当前数据返回的是网络(2)的信息
		if(flag2 && net_type2.equals(net_type)){
			sinr = sinr2;
			net_type = net_type2;
			signal_strength = signal_strength2;
			lac_tac = lac2;
			cid_pci = cid2;
		}
		if(!(flag1 && net_type.equals(net_type1)) && !(flag2 && net_type.equals(net_type2))){
			if(tac!=null && !tac.equals("")){
				lac_tac = tac;
			}
			if(pci!=null && !pci.equals("")){
				cid_pci = pci;
			}
			if(tac!=null && !tac.equals("") && lac!=null && !lac.equals("")){
				lac_tac = lac+"|"+tac;
			}
			if(pci!=null && !pci.equals("") && cid!=null && !cid.equals("")){
				cid_pci = cid+"|"+pci;
			}
		}
		map.put("lac_tac",lac_tac);
		map.put("cid_pci",cid_pci);
		map.put("sinr",sinr);
		if(net_type.equals("WIFI")){
			map.put("signal_strength",wifi_ss);	
		}else{
			map.put("signal_strength",signal_strength);
		}
		map.put("net_type",net_type);
		
		map.put("sinr1", sinr1);
		map.put("net_type1", net_type1);
		map.put("signal_strength1", signal_strength1);
		map.put("lac_tac1", lac1);
		map.put("cid_pci1", cid1);
		
		map.put("sinr2", sinr2);
		map.put("net_type2", net_type2);
		map.put("signal_strength2", signal_strength2);
		map.put("lac_tac2", lac2);
		map.put("cid_pci2", cid2);
		
		String file_index = (String)dataMap.get("file_index");
		String system_version = "";
		if(model.toLowerCase().contains("iphone")){
			map.put("android_ios", "ios");	
		}else{
			map.put("android_ios", "android");
		}
		map.put("file_index", file_index);
		boolean flag = writeCsv(map,numType,longitudeNum,latitudeNum);
		System.out.println("写入csv数据："+flag);
	}
	
	public boolean writeCsv(Map map,String numType,double longitudeNum,double latitudeNum){
		String []str = getArrayFromMap(map);
		String keys[] = getKeysFromMap(map);
		Format fm = new DecimalFormat("#.######");
		String fileName = fm.format(longitudeNum)+"_"+fm.format(latitudeNum)+"_"+numType+".csv";
		String filePath = ConfParser.csvwritepath;
		String floderName = Math.floor(longitudeNum)+"_"+Math.floor(latitudeNum);
		String paths = filePath+File.separator+floderName+File.separator+fileName;
		if(filePath.isEmpty()){
//			filePath = ""; 
			System.err.println("未配置csv输出路径");
			return false;
		}
		try {
			File file = new File(paths);
			open(paths,keys);
			write(str);
			close();
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	public String[] getArrayFromMap(Map map){
		String [] strs = new String[map.size()];
		int i = 0;
		Collection collection = map.values();
		Object[] objects = collection.toArray(); 
		for (int j = 0; j < objects.length; j++) {
			String str = objects[j]==null?"":objects[j].toString();
			strs[i++]= str;
		}
		return strs;
	}
	public String[] getKeysFromMap(Map map){
		String [] keys = new String[map.size()];
		int i = 0;
		Set set = map.keySet();
		Object[] objects = set.toArray(); 
		for (int j = 0; j < objects.length; j++) {
			String str = objects[j].toString();
			keys[i++]= str;
		}
		return keys;
	}
	//添加表头
	public void open(String fileName,String[] keys) {
		boolean flag = false;
		csvFile = new File(fileName);
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
				flag = true;
			}
			writer = new BufferedWriter(new FileWriterWithEncoding(csvFile,"gbk", true));
			cwriter = new CsvWriter(writer,',');
			if(flag){
				write(keys);//写入表头
			}
		} catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write(String[] dataStr) {
		try {
            cwriter.writeRecord(dataStr, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		
		try {
			if (writer != null) {
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (cwriter != null) {
				cwriter.flush();
				cwriter.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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
