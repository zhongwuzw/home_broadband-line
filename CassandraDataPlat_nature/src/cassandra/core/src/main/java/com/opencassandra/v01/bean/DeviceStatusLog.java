package com.opencassandra.v01.bean;

import java.util.Date;

/**
 * @author wjwd
 */
public class DeviceStatusLog {
	private Integer id;// ID
	private Integer bid;// DeviceInfo关联ID
	private String deviceState;// 设备状态
	private String operator;// 运营商或运行环境介绍
	private String testState;// 设备测试运行状态
	private String testInfo;// 设备测试信息
	private String testType;// 运行任务类型
	private String apn;// 设备运行apn
	private String ipv4;// 设备运行IPV4地址
	private String ipv6;// 设备运行IPV6地址
	private String imsi;// 设备IMSI号码
	private String mtmsi;// 设备运行时的MTMSI号码
	private String currentPort;// 设备当前运行端口号
	private String pci;// 设备运行时，无线侧PCI参数
	private String tm;// 设备运行时，无线侧TM参数
	private String tac;// 设备运行时，无线侧TAC参数
	private String sinr;// 设备运行时，无线侧SINR参数
	private String rsrp;// 设备运行时，无线侧RSRP参数
	private String cellId;// 设备运行时，无线侧小区ID参数
	private String nCellId;// 设备运行时，无线侧第二邻区ID参数
	private String cpu;// 设备CPU运行状态
	private String memory;// 设备内存占用状态
	private String io;// 设备I/O运行状态
	private String battery;// 设备电池状态
	private Integer isCharging;// 设备是否在充电
	private Integer runApps;// 设备上运行的APP数量
	private Double longitude;// 经度
	private Double latitude;// 纬度
	private Double height;// 高度
	private Double velocity;// 速率
	private Date updateTime;// 更新时间
	// private DeviceInfo deviceInfo;

	public DeviceStatusLog() {
		super();
	}

	public DeviceStatusLog(Integer id, Integer bid, String deviceState,
			String operator, String testState, String testInfo,
			String testType, String apn, String ipv4, String ipv6, String imsi,
			String mtmsi, String currentPort, String pci, String tm,
			String tac, String sinr, String rsrp, String cellId,
			String nCellId, String cpu, String memory, String io,
			String battery, Integer isCharging, Integer runApps,
			Double longitude, Double latitude, Double height, Double velocity,
			Date updateTime) {
		super();
		this.id = id;
		this.bid = bid;
		this.deviceState = deviceState;
		this.operator = operator;
		this.testState = testState;
		this.testInfo = testInfo;
		this.testType = testType;
		this.apn = apn;
		this.ipv4 = ipv4;
		this.ipv6 = ipv6;
		this.imsi = imsi;
		this.mtmsi = mtmsi;
		this.currentPort = currentPort;
		this.pci = pci;
		this.tm = tm;
		this.tac = tac;
		this.sinr = sinr;
		this.rsrp = rsrp;
		this.cellId = cellId;
		this.nCellId = nCellId;
		this.cpu = cpu;
		this.memory = memory;
		this.io = io;
		this.battery = battery;
		this.isCharging = isCharging;
		this.runApps = runApps;
		this.longitude = longitude;
		this.latitude = latitude;
		this.height = height;
		this.velocity = velocity;
		this.updateTime = updateTime;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getBid() {
		return bid;
	}

	public void setBid(Integer bid) {
		this.bid = bid;
	}

	public String getDeviceState() {
		return deviceState;
	}

	public void setDeviceState(String deviceState) {
		this.deviceState = deviceState;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getTestState() {
		return testState;
	}

	public void setTestState(String testState) {
		this.testState = testState;
	}

	public String getTestInfo() {
		return testInfo;
	}

	public void setTestInfo(String testInfo) {
		this.testInfo = testInfo;
	}

	public String getTestType() {
		return testType;
	}

	public void setTestType(String testType) {
		this.testType = testType;
	}

	public String getApn() {
		return apn;
	}

	public void setApn(String apn) {
		this.apn = apn;
	}

	public String getIpv4() {
		return ipv4;
	}

	public void setIpv4(String ipv4) {
		this.ipv4 = ipv4;
	}

	public String getIpv6() {
		return ipv6;
	}

	public void setIpv6(String ipv6) {
		this.ipv6 = ipv6;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getMtmsi() {
		return mtmsi;
	}

	public void setMtmsi(String mtmsi) {
		this.mtmsi = mtmsi;
	}

	public String getCurrentPort() {
		return currentPort;
	}

	public void setCurrentPort(String currentPort) {
		this.currentPort = currentPort;
	}

	public String getPci() {
		return pci;
	}

	public void setPci(String pci) {
		this.pci = pci;
	}

	public String getTm() {
		return tm;
	}

	public void setTm(String tm) {
		this.tm = tm;
	}

	public String getTac() {
		return tac;
	}

	public void setTac(String tac) {
		this.tac = tac;
	}

	public String getSinr() {
		return sinr;
	}

	public void setSinr(String sinr) {
		this.sinr = sinr;
	}

	public String getRsrp() {
		return rsrp;
	}

	public void setRsrp(String rsrp) {
		this.rsrp = rsrp;
	}

	public String getCellId() {
		return cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}

	public String getnCellId() {
		return nCellId;
	}

	public void setnCellId(String nCellId) {
		this.nCellId = nCellId;
	}

	public String getCpu() {
		return cpu;
	}

	public void setCpu(String cpu) {
		this.cpu = cpu;
	}

	public String getMemory() {
		return memory;
	}

	public void setMemory(String memory) {
		this.memory = memory;
	}

	public String getIo() {
		return io;
	}

	public void setIo(String io) {
		this.io = io;
	}

	public String getBattery() {
		return battery;
	}

	public void setBattery(String battery) {
		this.battery = battery;
	}

	public Integer getIsCharging() {
		return isCharging;
	}

	public void setIsCharging(Integer isCharging) {
		this.isCharging = isCharging;
	}

	public Integer getRunApps() {
		return runApps;
	}

	public void setRunApps(Integer runApps) {
		this.runApps = runApps;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getHeight() {
		return height;
	}

	public void setHeight(Double height) {
		this.height = height;
	}

	public Double getVelocity() {
		return velocity;
	}

	public void setVelocity(Double velocity) {
		this.velocity = velocity;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	// public DeviceInfo getDeviceInfo() {
	// return deviceInfo;
	// }
	//
	// public void setDeviceInfo(DeviceInfo deviceInfo) {
	// this.deviceInfo = deviceInfo;
	// }

}
