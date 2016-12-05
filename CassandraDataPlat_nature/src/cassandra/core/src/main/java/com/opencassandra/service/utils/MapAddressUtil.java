package com.opencassandra.service.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.lang.StringUtils;

import com.opencassandra.descfile.ConfParser;

public class MapAddressUtil {

	/**
	 * 通过经纬度获取到地址信息
	 * 
	 * @param x
	 * @param y
	 * @return
	 * @throws IOException
	 * @return String
	 */
	public static String testPost(String x, String y) throws IOException {

		URL url = new URL("http://api.map.baidu.com/geocoder?ak=" + ConfParser.ak + "&coordtype=wgs84ll&location=" + x + "," + y + "&output=json");
		// URL url = new URL("http://www.baidu.com");
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
			int forStart = str.indexOf("formatted_address\":");
			int forEnd = str.indexOf(",\"business");
			
			int cStart = str.indexOf("city\":");
			int cEnd = str.indexOf(",\"direction");
			// int cEnd = str.indexOf(",\"district");

			int dStart = str.indexOf(",\"district");
			int dEnd = str.indexOf(",\"province");

			int pStart = str.indexOf(",\"province");
			int pEnd = str.indexOf(",\"street");

			int sStart = str.indexOf(",\"street");
			int sEnd = str.indexOf(",\"street_number");

			int snStart = str.indexOf("street_number");

			int ccStart = str.indexOf(",\"cityCode");

			
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

			if (ccStart > 0) {
				String ccStr = str.substring(ccStart);
				if (ccStr != null && ccStr.length() > 0) {
					int ccEnd = ccStart + 12;
					String ccStr1 = str.substring(ccEnd);
					System.out.println(ccStr1);
					int ccIndex_end = ccStr1.indexOf("}");
					String city_Code = ccStr1.substring(0, ccIndex_end).trim();
					if (StringUtils.isNotBlank(city_Code)) {
						ssq += city_Code + "_";
					} else {
						ssq += "-_";
					}
				}
			} else {
				ssq += "-_";
			}
			
			if (forStart > 0 && forEnd > 0) {
				String formatted_address = str.substring(forStart + 20, forEnd - 1);
				if (StringUtils.isNotBlank(formatted_address)) {
					
					ssq += formatted_address + "_";
				} else {
					System.out.println(str + "--->请求百度失败");
					ssq += "-_";
				}
			} else {
				ssq += "-_";
			}
			System.out.println(ssq+" :::::::::::::::");

			if (ssq.endsWith("_")) {
				ssq = ssq.substring(0, ssq.length() - 1);
			}
			return ssq;
		}
		return null;
	}

}
