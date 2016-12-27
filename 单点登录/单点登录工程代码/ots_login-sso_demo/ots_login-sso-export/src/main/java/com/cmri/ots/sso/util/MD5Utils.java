package com.cmri.ots.sso.util;

import java.io.IOException;
import java.security.MessageDigest;

public class MD5Utils {

	public final static String MD5(String s) {
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'a', 'b', 'c', 'd', 'e', 'f' };
		try {
			byte[] strTemp = s.getBytes("UTF-8");
			MessageDigest mdTemp = MessageDigest.getInstance("MD5");
			mdTemp.update(strTemp);
			byte[] md = mdTemp.digest();
			int j = md.length;
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte b = md[i];
				// System.out.println((int)b);
				str[k++] = hexDigits[b >> 4 & 0xf];
				str[k++] = hexDigits[b & 0xf];
			}
			return new String(str);
		} catch (Exception e) {
			return null;
		}
	}

	public static String encode(byte[] bstr) {

		return new sun.misc.BASE64Encoder().encode(bstr);

	}

	public static byte[] decode(String str) {

		byte[] bt = null;

		try {

			sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();

			bt = decoder.decodeBuffer(str);

		} catch (IOException e) {

			e.printStackTrace();

		}

		return bt;

	}

	public static int getRandom() {
		java.util.Random r = new java.util.Random();
		return r.nextInt();
	}
}
