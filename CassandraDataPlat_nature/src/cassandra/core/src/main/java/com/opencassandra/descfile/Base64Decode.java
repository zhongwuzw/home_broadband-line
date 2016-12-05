package com.opencassandra.descfile;

import java.net.URLDecoder;
import java.util.regex.Pattern;

import sun.misc.BASE64Decoder;

public class Base64Decode {

	/**
	 * 将字符串英文大小写转换author:黄流洋 date:2015-10-15
	 * 
	 * @param dbuser传入需要处理的用户名或密码
	 * @return
	 */
	public static String getStringToUpOrLow(String dbuser) {
		String up_Low_string = dbuser;
		String[] u_l_array = up_Low_string.split("");
		System.out.println("before-----" + up_Low_string);
		StringBuffer again_username = new StringBuffer();
		String low_regu = "^[a-z]+$";
		String up_regu = "^[A-Z]+$";
		int i = 0;
		for (int j = u_l_array.length; i < j; ++i) {
			String arr = u_l_array[i];
			if ((arr != null) && (!("".equals(arr)))) {
				if (Pattern.matches(low_regu, arr))
					arr = arr.toUpperCase();
				else if (Pattern.matches(up_regu, arr))
					arr = arr.toLowerCase();

				again_username.append(arr);
			}
		}
		System.out.println("after-----" + again_username.toString());
		String user_pass = getFromBase64(again_username.toString());
		return user_pass;
	}

	/**
	 * 将 BASE64 编码的字符串 s 进行解码 author:黄流洋 date:2015-10-15
	 * 
	 * @param s
	 * @return
	 */
	public static String getFromBase64(String s) {
		if (s == null)
			return null;
		BASE64Decoder decoder = new BASE64Decoder();
		try {
			byte[] b = decoder.decodeBuffer(s);
			// 对一些特殊的字符在处理author:黄流洋 date:2016-05-10
			String b__ = URLDecoder.decode(new String(b), "utf-8");
			System.out.println(b__);
			return b__;
		} catch (Exception e) {
		}
		return null;
	}

	// cm9vdA==
	// Y21yaWN0cDIzMw==

	public static void main(String[] args) {
		String ss = getStringToUpOrLow("z2jHC2uYmdeXmduZmq==");
		System.out.println(ss + ":::::::");
	}

}
