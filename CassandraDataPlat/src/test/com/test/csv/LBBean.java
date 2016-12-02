package com.test.csv;

public class LBBean {
	
	private int longtitudeStart;
	
	private int longtitudeEnd;
	
	private int latitudeStart;
	
	private int latitudeEnd;

	public LBBean(int longtitudeStart, int longtitudeEnd,	int latitudeStart, int latitudeEnd) {
		this.longtitudeStart = longtitudeStart;
		this.longtitudeEnd = longtitudeEnd;
		this.latitudeStart = latitudeStart;
		this.latitudeEnd = latitudeEnd;
	}

	public double getLongtitudeStart() {
		return longtitudeStart;
	}

	public void setLongtitudeStart(int longtitudeStart) {
		this.longtitudeStart = longtitudeStart;
	}

	public double getLongtitudeEnd() {
		return longtitudeEnd;
	}

	public void setLongtitudeEnd(int longtitudeEnd) {
		this.longtitudeEnd = longtitudeEnd;
	}

	public double getLatitudeStart() {
		return latitudeStart;
	}

	public void setLatitudeStart(int latitudeStart) {
		this.latitudeStart = latitudeStart;
	}

	public double getLatitudeEnd() {
		return latitudeEnd;
	}

	public void setLatitudeEnd(int latitudeEnd) {
		this.latitudeEnd = latitudeEnd;
	}
}