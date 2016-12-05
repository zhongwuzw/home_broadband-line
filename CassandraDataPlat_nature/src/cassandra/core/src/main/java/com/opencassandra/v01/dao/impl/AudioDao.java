package com.opencassandra.v01.dao.impl;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.csvreader.CsvReader;
import com.opencassandra.descfile.ConfParser;



public class AudioDao{
	Statement statement;
	Connection conn;
	StringBuffer sql = new StringBuffer();
	
	public static void main(String[] args) {
		AudioDao ad = new AudioDao();
		ad.start();
		List l = ad.query("863583024375702", "00000000_05004.2876_0-2014_12_30_19_03_49_900");
		for (int i = 0; i < l.size(); i++) {
			HashMap map =(HashMap)l.get(i);
			Set set = map.keySet();
			Iterator iter = set.iterator();
			while(iter.hasNext()){
				String name = (String)iter.next();
				System.out.println(name+","+map.get(name));
			}
		}
		String audio_name = "";
		String audio_path = "";
		String data_state = "";
		String store_path = "";
		String store_path_rela = "";
		if(l!=null){
			HashMap map =(HashMap)l.get(0);
			audio_name = map.get("name")==null?"":map.get("name").toString();
			audio_path = map.get("load_url")==null?"":map.get("load_url").toString();
			data_state = map.get("data_state")==null?"":map.get("data_state").toString();
			store_path = map.get("store_path")==null?"":map.get("store_path").toString();
			store_path_rela = map.get("store_path_rela")==null?"":map.get("store_path_rela").toString();
		}
		System.out.println(audio_name+","+audio_path+","+data_state+","+store_path+","+store_path_rela);
		try {
			ad.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void start() {
		String driver = "com.mysql.jdbc.Driver";
		String url = ConfParser.url;
		String user = ConfParser.user;
//		String password = "cmrictpdata";
		String password = ConfParser.password;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, password);
			statement = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() throws SQLException {
		if (conn != null) {
			conn.close();
		}
	}

	public boolean insert(String sql) {
		System.out.println("sql:" + sql);
		boolean flag = false;
		try {
			flag = statement.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return flag;
	}

	public String getOrgKeyById(String id){
		start();
		return "";
	}
	
	public List query(String imei,String prefix) {
		start();
//		String sql = "select name,load_url from ctpstage02.d_audio_sample where id  = 1";
		String sql = "SELECT * FROM report.d_specified_file_upload file LEFT JOIN ctpstage02.d_audio_sample audio on file.audio_id = audio.id LEFT JOIN device.d_device device on file.terminal_sn = device.device_imei where file.terminal_sn = '"+imei+"' and file.data_mark = '"+prefix+"'";
		List list = new ArrayList();
		try {
			System.out.println(sql);
			ResultSet rs = statement.executeQuery(sql);
			ResultSetMetaData md = rs.getMetaData();
			int columnCount = md.getColumnCount();
			while (rs.next()) {
				Map map = new HashMap();
				for (int i = 1; i <= columnCount; i++) {
					map.put(md.getColumnName(i), rs.getObject(i));
				}
				list.add(map);
			}
			if(list.size()>0){
				updateMos(prefix,list);	
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	private void updateMos(String prefix,List list){
		String data_id = "";
		String data_type = "";
		String data_mark = "";
		String data_code = "";
		String store_path = "";
		String store_path_rela = ""; 
		
		HashMap map = (HashMap)list.get(0);
		data_id = map.get("data_id")==null?"":map.get("data_id").toString();
		data_type = map.get("data_type")==null?"":map.get("data_type").toString();
		data_mark = map.get("data_mark")==null?"":map.get("data_mark").toString();
		data_code = map.get("data_code")==null?"":map.get("data_code").toString();
		store_path = map.get("store_path")==null?"":map.get("store_path").toString();
		store_path_rela = map.get("store_path_rela")==null?"":map.get("store_path_rela").toString();
		
		String sql = "select * from report.mos where data_id = '"+data_id+"' and data_type = '"+data_type+"' and data_mark = '"+data_mark+"' and data_code = '"+data_code+"' and store_path = '"+store_path+"' and store_path_rela = '"+store_path_rela+"'";
		try {
			ResultSet rs = statement.executeQuery(sql);
			if(!rs.next()){
				File file = new File(store_path_rela.contains(";")?store_path_rela.split(";")[0]:store_path_rela);
				String [] columnKey = new String[]{"data_id","data_type","data_mark","data_code","store_path","store_path_rela"};
				String [] columnValue = new String[]{data_id,data_type,data_mark,data_code,store_path,store_path_rela};
//				File file = new File("C:\\Users\\issuser\\Desktop\\test\\err3\\test\\00000000_05004_0-2014_12_30_19_08_13_461.mos.csv");
				if(!file.exists() && file.isFile()){
					return ;
				}
				ArrayList<String[]> data_list = readCsv(file);
				String title[] = data_list.get(0);
				String data[] = data_list.get(1);
				StringBuffer insert_sql = new StringBuffer("insert into report.mos (");
				for (int i = 0; i < columnKey.length; i++) {
					if(i==title.length-1){
						insert_sql.append(columnKey[i]);
					}else{
						insert_sql.append(columnKey[i]+",");
					}
				}
				for (int i = 0; i < title.length; i++) {
					String t = title[i];
					if(t.contains(" ")){
						t = t.replace(" ", "");
					}
					if(t.contains("(ms)")){
						t = t.replace("(ms)", "");
					}
					System.out.println(t);
					if(i==title.length-1){
						if(t.isEmpty()){
//							insert_sql.append("'"+title[i]+"')");
						}else{
							insert_sql.append(t);
						}
						if(insert_sql.toString().endsWith(",")){
							insert_sql = new StringBuffer(insert_sql.substring(0,insert_sql.length()-1));
						}
					}else{
						insert_sql.append(t+",");
					}
				}
				insert_sql.append(") values (");
				for (int i = 0; i < columnValue.length; i++) {
					if(i==title.length-1){
						insert_sql.append("'"+columnValue[i]+"'");
					}else{
						insert_sql.append("'"+columnValue[i]+"',");
					}
				}
				for (int i = 0; i < data.length; i++) {
					if(i==data.length-1){
						if(data[i].isEmpty()){
						}else{
							insert_sql.append("'"+data[i]+"'");
						}
						if(insert_sql.toString().endsWith(",")){
							insert_sql = new StringBuffer(insert_sql.substring(0,insert_sql.length()-1));
						}
					}else{
						insert_sql.append("'"+data[i]+"',");
					}
				}
				insert_sql.append(")");
				System.out.println(insert_sql);
				statement.executeUpdate(insert_sql.toString());
			}
		} catch (SQLException e) {
			System.out.println("执行更新mos表出错");
			e.printStackTrace();
		}
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
}
