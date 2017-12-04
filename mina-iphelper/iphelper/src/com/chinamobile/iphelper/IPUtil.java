package com.chinamobile.iphelper;

public class IPUtil {
	public static long ipToLong(String ipAddress) {
		long result = 0;
		String[] ipAddressInArray = ipAddress.split("\\.");
		for (int i = 3; i >= 0; i--) {
			long ip = Long.parseLong(ipAddressInArray[3 - i]);
			// left shifting 24,16,8,0 and bitwise OR
			// 1. 192 << 24
			// 1. 168 << 16
			// 1. 1 << 8
			// 1. 2 << 0
			result |= ip << (i * 8);
		}
		return result;
	}

	// ip = 3232235778
	public static String longToIp(long ip) {
		StringBuilder result = new StringBuilder(15);
		for (int i = 0; i < 4; i++) {
			result.insert(0, Long.toString(ip & 0xff));
			if (i < 3) {
				result.insert(0, '.');
			}
			ip = ip >> 8;
		}
		return result.toString();
	}

	// ip = 3232235778
	public String longToIp2(long ip) {
		return ((ip >> 24) & 0xFF) + "." + ((ip >> 16) & 0xFF) + "." + ((ip >> 8) & 0xFF) + "." + (ip & 0xFF);
	}
}
