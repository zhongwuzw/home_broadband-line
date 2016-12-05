package com.opencassandra.v01.dao.impl;

public class BdFix {

	private static double x_pi = 3.14159265358979324*3000.0/180.0;

	static public double[] bd_encrypt(double gg_lon, double gg_lat){
		double bd_lat, bd_lon;
	    double x = gg_lon, y = gg_lat;
	    double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi);
	    double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi);
	    bd_lon = z * Math.cos(theta) + 0.0065;
	    bd_lat = z * Math.sin(theta) + 0.006;
	    double[] bdPoint = {bd_lon,bd_lat};
	    return bdPoint;
	}

	static public double[] bd_decrypt(double bd_lon, double bd_lat){
		double gg_lat, gg_lon;
	    double x = bd_lon - 0.0065, y = bd_lat - 0.006;
	    double z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * x_pi);
	    double theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * x_pi);
	    gg_lon = z * Math.cos(theta);
	    gg_lat = z * Math.sin(theta);
	    double[] marsPoint = {gg_lon,gg_lat};
	    return marsPoint;
	}
}

