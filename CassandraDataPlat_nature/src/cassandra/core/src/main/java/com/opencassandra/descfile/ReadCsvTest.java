package com.opencassandra.descfile;

import java.io.File;
import java.util.regex.Pattern;

public class ReadCsvTest {

	public static void main(String[] args) {
		String imei = "";
		String fileName = "";
		String numType = "";
		String keyspace = "";
		String model = "";//型号
		String mobile = "";
		String path = "";
		//keyspace/numType/Mobile/model/imei

		String filePath[] = new String[]{"C:\\Users\\issuser\\Desktop\\DateFile\\00000000_01001.000_00000000-2014_09_04_18_03_02_217-TCP.abstract.csv-_-CMCC_ZB_SHICHANG_PRJ_E2E_ABEIZHOU-_-others-_-Mobile-_-SM-N9008V-_-EMPTY-_-358584058718876-_-657","C:\\Users\\issuser\\Desktop\\DateFile\\00000000_02001.1874_0-2014_09_04_09_47_25_683.abstract.csv-_-CMCC_ZB_SHICHANG_PRJ_E2E_BAOJI-_-browse-_-Mobile-_-K-Touch Tou ch 1-_-EMPTY-_-862302020404940-_-707","C:\\Users\\issuser\\Desktop\\DateFile\\00000000_02001.1874_0-2014_09_04_09_47_25_683.monitor.csv-_-CMCC_ZB_SHICHANG_PRJ_E2E_BAOJI-_-browse-_-Mobile-_-K-Touch Tou ch 1-_-EMPTY-_-862302020404940-_-707","C:\\Users\\issuser\\Desktop\\DateFile\\00000000_02001.1874_0-2014_09_04_10_09_10_941.abstract.csv-_-CMCC_ZB_SHICHANG_PRJ_E2E_BAOJI-_-browse-_-Mobile-_-K-Touch Tou ch 1-_-EMPTY-_-862302020404940-_-707","C:\\Users\\issuser\\Desktop\\DateFile\\00000000_01001.000_00000000-2014_09_04_18_03_02_217-TCP.abstract.csv-_-CMCC_ZB_SHICHANG_PRJ_E2E_ABEIZHOU-_-others-_-Mobile-_-SM-N9008V-_-EMPTY-_-358584058718876-_-657","C:\\Users\\issuser\\Desktop\\DateFile\\00000000_02001.1874_0-2014_09_04_13_55_28_573.abstract.csv-_-CMCC_ZB_SHICHANG_PRJ_E2E_BAOJI-_-browse-_-Mobile-_-HUAWEI G716-L070-_-EMPTY-_-862372021044367-_-707"};
		Pattern pattern1 = Pattern.compile("[-_.]");
		Pattern pattern2 = Pattern.compile("\\d+");
		String prefix = "C:\\Users\\issuser\\Desktop\\";
		for (int i = 0; i < filePath.length; i++) {
			File file = new File(filePath[i]);
//			keyspace = file.getAbsolutePath().replace(prefix, "");
//			keyspace = keyspace.substring(0, keyspace.indexOf(File.separator));
			String name = file.getName();
//			if(!name.contains(".summary.csv")){
//				continue;
//			}
			String data[] = name.split("-_-");
			fileName = data[0];
			keyspace = data[1];
			numType = data[2];
			mobile = data[3];
			model = data[4];
			imei = data[6];
			
			path = keyspace+File.separator+numType+File.separator+mobile+File.separator+model+File.separator+imei+File.separator+fileName;
			String str[] = pattern1.split(name);
			
			numType = str[1];
			System.out.println(path);
			System.out.println("keyspace:"+keyspace+",imei:"+imei+",mobile:"+mobile+",model:"+model+",numType:"+numType+",fileName:"+fileName);
		}
	}
}
