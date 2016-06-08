package cn.speed.history.process.impl;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.speed.history.dao.ResultUploadDao;
import cn.speed.history.dao.SpeedtestInsertDao;
import cn.speed.history.process.DealWithCsv;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class DealWithSpeedtestAvg implements DealWithCsv{

	Map<String,Object> resultSet = new HashMap<String, Object>();
	
	
	public Map<String, Object> getResultSet() {
		return resultSet;
	}

	/**
	 * 读取deviceInfo.CSV文件
	 */
	public Map<String,Object> readInfoCsv(String filePath) {
		Map<String,Object> contents = new HashMap<String, Object>();
		CsvReader reader = null;
		try {
			ArrayList<String[]> csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath;
			reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				csvList.add(reader.getValues());
			}
			
			contents.put("mfr",csvList.get(5)[1] != null ? csvList.get(5)[1] : "");
			contents.put("imei",csvList.get(6)[1] != null ? csvList.get(6)[1] : "");
			contents.put("sim_state",csvList.get(14)[1] != null ? csvList.get(14)[1] : "");
			

			contents.put("net_state",csvList.get(17)[1] != null ? csvList.get(17)[1] : "");
			contents.put("operator",csvList.get(13)[1] != null ? csvList.get(13)[1] : "");
			contents.put("roaming",csvList.get(18)[1] != null ? csvList.get(18)[1] : "");
			
			contents.put("ots_test_position",csvList.get(24)[1] != null ? csvList.get(24)[1] : "");
			contents.put("inside_ip",csvList.get(28)[1] != null ? csvList.get(28)[1] : "");
			contents.put("outside_ip",csvList.get(29)[1] != null ? csvList.get(29)[1] : "");
			contents.put("mac",csvList.get(30)[1] != null ? csvList.get(30)[1] : "");
			
			contents.put("l_net_type",csvList.get(25)[1] != null ? csvList.get(25)[1] : "");
			contents.put("l_net_name",csvList.get(26)[1] != null ? csvList.get(26)[1] : "");
			contents.put("lac",csvList.get(20)[1] != null ? csvList.get(20)[1] : "");
			contents.put("cid",csvList.get(21)[1] != null ? csvList.get(21)[1] : "");
			contents.put("tm_model",csvList.get(1)[1] != null ? csvList.get(1)[1] : "");
			
			
			contents.put("os_ver",csvList.get(3)[1] != null ? csvList.get(3)[1] : "");
			contents.put("ots_ver",csvList.get(4)[1] != null ? csvList.get(4)[1] : "");
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if(reader!=null){
				reader.close();
			}
		}
		return contents;
	}
	
	/**
	 * 读取Detail.CSV文件
	 */
	public Map<String,Object> readDetailCsv(String filePath,Integer file_id) {

		Map<String,Object> contents = new HashMap<String, Object>();
		CsvReader reader = null;
		ArrayList<String[]> csvList = null;
		try {
			csvList = new ArrayList<String[]>(); // 用来保存数据
			String csvFilePath = filePath;
			reader = new CsvReader(csvFilePath, ',',
					Charset.forName("GB2312")); // 一般用这编码读就可以了

			// reader.readHeaders(); // 跳过表头 如果需要表头的话，不要写这句。

			while (reader.readRecord()) { // 逐行读入除表头的数据
				csvList.add(reader.getValues());
			}
			if(csvList.get(0)[1] == null && !"".equals(csvList.get(0)[1])){
				 SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
				 csvList.get(0)[1] = simpleDateFormat.format(new Date());
			}
			contents.put("update_time",csvList.get(0)[1] != null && !"".equals(csvList.get(0)[1]) ? csvList.get(0)[1] : "");
			contents.put("machine_model",csvList.get(1)[1] != null && !"".equals(csvList.get(1)[1]) ? csvList.get(1)[1] : "--");
			contents.put("hw_ver",csvList.get(2)[1] != null && !"".equals(csvList.get(2)[1]) ? csvList.get(2)[1] : "--");
			contents.put("speed_test_position",csvList.get(3)[1] != null  && !"".equals(csvList.get(3)[1]) ? csvList.get(3)[1] : "--");
			contents.put("d_net_name",csvList.get(6)[1] != null && !"".equals(csvList.get(6)[1]) ? csvList.get(6)[1] : "--");
			contents.put("d_net_type",csvList.get(5)[1] != null && !"".equals(csvList.get(5)[1]) ? csvList.get(5)[1] : "--");
			contents.put("server_info",csvList.get(7)[1] != null && !"".equals(csvList.get(0)[1]) ? csvList.get(7)[1] : "--");
			contents.put("tm_inside_ip",csvList.get(8)[1] != null && !"".equals(csvList.get(8)[1]) ? csvList.get(8)[1] : "--");
			contents.put("tm_outside_ip",csvList.get(9)[1] != null && !"".equals(csvList.get(9)[1]) ? csvList.get(9)[1] : "--");
			contents.put("tm_mac",csvList.get(10)[1] != null && !"".equals(csvList.get(10)[1]) ? csvList.get(10)[1] : "--");
			if(csvList.size()==16){
				String	test_times = csvList.get(0)[1].substring(0,10).replaceAll("_", "-")+" "+ csvList.get(0)[1].substring(11).replaceAll("_", ":");
				String[]str = new String[]{test_times,"--","--","--","--","-1","-1","-1","-1","-1","-1"}; 
				csvList.add(str);
			}
			contents.put("protocol_type",csvList.get(11)[0] != null && !"".equals(csvList.get(11)[0]) ? csvList.get(11)[0] : "--");
			contents.put("time_delay",csvList.get(11)[1] != null && !"".equals(csvList.get(11)[1]) ? csvList.get(11)[1] : "-1");
			if(csvList.get(11)[2] != null && !"".equals(csvList.get(11)[2]) && csvList.get(11)[2].contains("Kbps")){
				double down = Double.valueOf(csvList.get(11)[2].replace("Kbps", ""))/1024;
				contents.put("down_velocity",String.valueOf(down));
			} else if(csvList.get(11)[2] != null && !"".equals(csvList.get(11)[2]) && csvList.get(11)[2].contains("Mbps")){
				contents.put("down_velocity",csvList.get(11)[2].replace("Mbps", ""));
			}else{
				contents.put("down_velocity","-1");
			}
			
			if(csvList.get(11)[3] != null && !"".equals(csvList.get(11)[3]) && csvList.get(11)[3].contains("Kbps")){
				double down = Double.valueOf(csvList.get(11)[3].replace("Kbps", ""))/1024;
				contents.put("up_velocity",String.valueOf(down));
			} else if(csvList.get(11)[3] != null && !"".equals(csvList.get(11)[3]) && csvList.get(11)[3].contains("Mbps")){
				contents.put("up_velocity",csvList.get(11)[3].replace("Mbps", ""));
			}else{
				contents.put("up_velocity","-1");
			}
			/** -----sundongyang----- */
			contents.put("file_id",file_id);
			StringBuffer sb = new StringBuffer();
			for(int i=15 ; i<csvList.size();i++){
				String[] csvStr = csvList.get(i);
				if(csvStr!=null && csvStr.length>0){
					if("TCP下行测速结果".equals(csvStr[0])){
						break;
					}
					for(int j = 0; j<csvStr.length;j++){
						sb.append(csvStr[j]+" ");
					}
					sb.append("\n");
				}
				
			}
			contents.put("log_detail",sb);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if(reader!=null){
				reader.close();
			}
		}
		return contents;
	}

	/**
	 * 读取Detail.CSV文件
	 */
	public void insertData(String infoCsv, String detailCsv,Integer file_id){
		
		if(!(new File(infoCsv)).exists() || !(new File(detailCsv)).exists()){
			return;
		}
		
		Map<String,Object> infoResultSet = readInfoCsv(infoCsv);
		Map<String,Object> detailResultSet = readDetailCsv(detailCsv,file_id);
		
		Iterator<String> itInfoResultSet=infoResultSet.keySet().iterator();  
		while (itInfoResultSet.hasNext()){
			String key = itInfoResultSet.next();
			resultSet.put(key, infoResultSet.get(key));
		}
		
		Iterator<String> itDetailResultSet=detailResultSet.keySet().iterator();  
		while (itDetailResultSet.hasNext()){
			String key = itDetailResultSet.next();
			resultSet.put(key, detailResultSet.get(key));
		}
		System.out.println(resultSet.size());
	}
	/**
	 * 写入CSV文件
	 */
	public void writeCsv(String[] contents) {
		try {
			String csvFilePath = "c:/test(ZONGTIPINGJUN)avg.csv";
			CsvWriter wr = new CsvWriter(csvFilePath, ',',
					Charset.forName("GB2312"));
			wr.writeRecord(contents);
			wr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean findFile(Map<String,Object> resultSet){
		String filesPath = (String)resultSet.get("store_path");
		Integer file_id = (Integer)resultSet.get("id");
		String[] filePath = filesPath.split(";");
		File deviceInfoFile = null;
		File detailsFile = null;
		File abstractFile = null;
		File monitorFile = null;
		boolean isLegal = true;
		
		for(int i=0; i<filePath.length; i++){
			if(filePath[i]!=null && !"".equals(filePath[i])){
				if(filePath[i].contains(".abstract.csv")){
					abstractFile = new File(filePath[i]);
					if(!abstractFile.exists()){
						isLegal = false;
						break;
					}
					deviceInfoFile = new File(abstractFile.getAbsolutePath().replace(".abstract.csv", ".deviceInfo.csv"));
					if(!deviceInfoFile.exists()){
						isLegal = false;
						break;
					}
					continue;
				}
			}
		}
		if(isLegal){
			insertData(deviceInfoFile.getAbsolutePath(),abstractFile.getAbsolutePath(),file_id);
			return true;
		}else{
			ResultUploadDao myRUD = new ResultUploadDao();
			if(Integer.parseInt( resultSet.get("isInsert").toString()) == 0){
				myRUD.updateById((Integer)(resultSet.get("id")),"3");
			}else if(Integer.parseInt( resultSet.get("isInsert").toString()) == 3){
				myRUD.updateById((Integer)(resultSet.get("id")),"4");
			}
		}
		return false;
	}

	public static void main(String[] args){
		SpeedtestInsertDao mySID = new SpeedtestInsertDao();
		DealWithSpeedtestAvg my = new DealWithSpeedtestAvg();
		//my.insertData("C:\\Users\\cmcc\\Desktop\\新建文件夹\\0_01001.288_0-2013_07_26_17_28_04_741.deviceInfo.csv","C:\\Users\\cmcc\\Desktop\\新建文件夹\\0_01001.288_0-2013_07_26_17_28_04_741.abstract.csv");

		mySID.insert(my.getResultSet());
	}
	
}
