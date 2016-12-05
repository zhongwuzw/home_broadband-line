package com.chinamobile.httpserver.data;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.log4j.Logger;
import org.jdom.*;
import org.jdom.input.*;
import org.xml.sax.InputSource;

import cn.speed.history.service.ResultUploadService;
import cn.speed.jdbc.utils.UploadXmlPara;

import com.chinamobile.util.MD5Builder;

public class DataParser {

	private static DataParser instance = null;
	static Logger logger = Logger.getLogger(DataParser.class);

	public static DataParser getInstance() {
		if (instance == null) {
			return new DataParser();
		}
		return instance;
	}

	private DataParser() {
	};

	public Map<String,Object> parseXMLContent(String contents) {
		Map<String,Object> list2insert = new HashMap<String,Object>();
		String org_id = "";//分组ID;
		String[] result = new String[2];
		StringBuffer sb = new StringBuffer();
		sb.append(MD5Builder.getMD5_AUTH_KEY());

		contents = contents.trim();
		ByteArrayInputStream xmlStream = null;
		try {
			xmlStream = new ByteArrayInputStream(contents.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//System.out.println(contents);
		SAXBuilder builder = new SAXBuilder();
		String dstAddress = "";
		try {
			Document doc = builder.build(xmlStream);
			Element rootEl = doc.getRootElement();
			//
			boolean isMobile = false;
			boolean isPc = false;
			if (contents.toLowerCase().contains(
					"<DevType>Mobile</DevType>".toLowerCase())) {
				isMobile = true;
			} else if (contents.toLowerCase().contains(
					"<DevType>PC</DevType>".toLowerCase())) {
				isPc = true;
			}
			List<Element> list = rootEl.getChildren();
			String uploadFile = ""; 
			for (Element ell : list) {
				if("IMEI".equalsIgnoreCase(ell.getName().toUpperCase().trim())){
							ResultUploadService service = new ResultUploadService();
							 List<Map<String, Object>> store_pathList = service.getStore_path(ell.getText().trim());
							 if(store_pathList.size()>0){
								  String store_path = (String)store_pathList.get(0).get("store_path");
								  if(store_path!="" && store_path!=null){
									  uploadFile = store_path;
								  }
								  org_id  = (String)store_pathList.get(0).get("org_id");
							 }
							break;
				}
			}
			if(uploadFile!=""){
				dstAddress = uploadFile;
			}else{
				dstAddress = GlobalPara.uploadfile ;
			}
			String device_type = "";
			String device_mac = "";
			String device_imei = "";
			for (Element el : list) {
				switch (UploadXmlPara
						.valueOf(el.getName().toUpperCase().trim())) {
					case TESTMODE:
						list2insert.put("test_mode", el.getText().trim() != null ? el.getText().trim().toLowerCase() : "");
						break;
					case DEVTYPE:
						list2insert.put("device_type", el.getText().trim() != null ? el.getText().trim() : "");
						device_type = el.getText().trim();
						break;
					case DEVMODEL:
						list2insert.put("device_model", el.getText().trim() != null ? el.getText().trim() : "");
						break;
					case MAC:
						list2insert.put("device_mac", el.getText().trim() != null ? el.getText().trim() : "");
						device_mac =  el.getText().trim();
						break;
					case IMEI:{
						list2insert.put("device_imei", el.getText().trim() != null ? el.getText().trim() : "");
						device_imei = el.getText().trim();
						break;
					}
					case CODE:
						list2insert.put("device_code", el.getText().trim() != null ? el.getText().trim() : "");
						break;
				}
				if (el.getName().toLowerCase().trim().equals("mac") && isMobile) {
					continue;
				}
				if (el.getName().toLowerCase().trim().equals("imei") && isPc) {
					continue;
				}
				if (el.getName().toLowerCase().trim().equals("code")) {
					continue;
				}
				dstAddress += el.getText().trim()
						+ System.getProperty("file.separator");
				sb.append(el.getText().trim());
			}
			/*
			 * 取消存储目录修改
			
			Date date= new Date();
			int month = date.getMonth()+1;
			int da  = date.getDate();
			if("PC".equalsIgnoreCase(device_type)){
				if("".equals(device_mac)){
					device_mac = "MAC";
				}
				dstAddress += (month+"月"+da+"日")+ System.getProperty("file.separator")+device_type+ System.getProperty("file.separator")+device_mac+System.getProperty("file.separator");
			}else if("Mobile".equalsIgnoreCase(device_type)){
				dstAddress += (month+"月"+da+"日")+ System.getProperty("file.separator")+device_type+ System.getProperty("file.separator")+device_imei+System.getProperty("file.separator");
			}
			 */
		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String uploadDate = Long.toString((new Date()).getTime());
		//logger.debug(uploadDate);

		/*
		 * 取消上传时间目录 dstAddress
		 * +=uploadDate.trim()+System.getProperty("file.separator");
		 */
		sb.append(uploadDate.trim());
		try {
			String updateCode = MD5Builder.getMD5(sb);
			//logger.info(updateCode);
			result[0] = updateCode;
			result[1] = dstAddress;
			list2insert.put("update_code", updateCode);
			list2insert.put("store_path_rela", dstAddress);
			list2insert.put("org_id", org_id);
			return list2insert;
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}