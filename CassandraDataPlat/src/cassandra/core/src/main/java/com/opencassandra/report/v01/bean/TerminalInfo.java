package com.opencassandra.report.v01.bean;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;

public class TerminalInfo {

	static Logger logger = Logger.getLogger(TerminalInfo.class);
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	
    private String key;
	private String deviceId;
	
	private String deviceTypeId;
	private String deviceTypeName;
	
	private String deviceOsId;
	private String deviceOsName;
	
	private String deviceImei;
	private String deviceMac;
	private String deviceNo;
	
	private String deviceToolVersionId;
	private String deviceToolVersionName;

	private String applyLocationName;
	private String applyDistrictName;
	
	private String deviceGroupId;
	private String deviceGroupName;
	
	private long lastUpdateTime;
	private long applyTime;

	private String description;
	private String remark;
	
	private long loginCounts;
	
	public enum TerminalInfoEnum{
		key, device_id, device_type_name, device_type_id, device_os_name, device_os_id, device_group_name, device_group_id,
		device_imei, device_mac, device_no, last_update_time, apply_time, apply_location_name, apply_district_name, description, 
		remark, device_tools_version_name, device_tools_version_id, login_counts;
	}	
	
	public TerminalInfo() {
		super();
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getDeviceTypeId() {
		return deviceTypeId;
	}

	public void setDeviceTypeId(String deviceTypeId) {
		this.deviceTypeId = deviceTypeId;
	}

	public String getDeviceTypeName() {
		return deviceTypeName;
	}

	public void setDeviceTypeName(String deviceTypeName) {
		this.deviceTypeName = deviceTypeName;
	}

	public String getDeviceOsId() {
		return deviceOsId;
	}

	public void setDeviceOsId(String deviceOsId) {
		this.deviceOsId = deviceOsId;
	}

	public String getDeviceOsName() {
		return deviceOsName;
	}

	public void setDeviceOsName(String deviceOsName) {
		this.deviceOsName = deviceOsName;
	}

	public String getDeviceImei() {
		return deviceImei;
	}

	public void setDeviceImei(String deviceImei) {
		this.deviceImei = deviceImei;
	}

	public String getDeviceMac() {
		return deviceMac;
	}

	public void setDeviceMac(String deviceMac) {
		this.deviceMac = deviceMac;
	}

	public String getDeviceNo() {
		return deviceNo;
	}

	public void setDeviceNo(String deviceNo) {
		this.deviceNo = deviceNo;
	}

	public String getDeviceToolVersionId() {
		return deviceToolVersionId;
	}

	public void setDeviceToolVersionId(String deviceToolVersionId) {
		this.deviceToolVersionId = deviceToolVersionId;
	}

	public String getDeviceToolVersionName() {
		return deviceToolVersionName;
	}

	public void setDeviceToolVersionName(String deviceToolVersionName) {
		this.deviceToolVersionName = deviceToolVersionName;
	}

	public String getApplyLocationName() {
		return applyLocationName;
	}

	public void setApplyLocationName(String applyLocationName) {
		this.applyLocationName = applyLocationName;
	}

	public String getApplyDistrictName() {
		return applyDistrictName;
	}

	public void setApplyDistrictName(String applyDistrictName) {
		this.applyDistrictName = applyDistrictName;
	}

	public String getDeviceGroupId() {
		return deviceGroupId;
	}

	public void setDeviceGroupId(String deviceGroupId) {
		this.deviceGroupId = deviceGroupId;
	}

	public String getDeviceGroupName() {
		return deviceGroupName;
	}

	public void setDeviceGroupName(String deviceGroupName) {
		this.deviceGroupName = deviceGroupName;
	}

	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(long lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}

	public long getApplyTime() {
		return applyTime;
	}

	public void setApplyTime(long applyTime) {
		this.applyTime = applyTime;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public long getLoginCounts() {
		return loginCounts;
	}

	public void setLoginCounts(long loginCounts) {
		this.loginCounts = loginCounts;
	}

	static public TerminalInfo toJavaBean(Row<String, String, ByteBuffer> myRow){
		logger.debug(myRow);
		
		TerminalInfo myTerminalInfo = new TerminalInfo();
		
		myTerminalInfo.setKey(myRow.getKey());
		
		List<HColumn<String, ByteBuffer>> columns = myRow.getColumnSlice()
				.getColumns();
		for (HColumn<String, ByteBuffer> hColumn : columns) {
			myTerminalInfo.setting(myTerminalInfo, hColumn.getName(), hColumn.getValueBytes());
		}
		return myTerminalInfo;
	}
	
	static public List<TerminalInfo> toJavaBean(List<Row<String, String, ByteBuffer>> myRows){
		ArrayList<TerminalInfo> myTerminalList = new ArrayList<TerminalInfo>();
		int i=0;
		for (Row<String, String, ByteBuffer> row : myRows) {
			logger.debug(row);
			TerminalInfo myTerminalInfo = new TerminalInfo();
			myTerminalInfo.setKey(row.getKey());
			List<HColumn<String, ByteBuffer>> columns = row.getColumnSlice()
					.getColumns();
			for (HColumn<String, ByteBuffer> hColumn : columns) {
				myTerminalInfo.setting(myTerminalInfo, hColumn.getName(), hColumn.getValueBytes());
			}
			myTerminalList.add(myTerminalInfo);
		}
		return myTerminalList;
	}
	
	static public String[] toColumns(){
		String[] columns = {"key", "device_id", "device_type_name", "device_type_id", "device_os_name", "device_os_id", "device_group_name", "device_group_id", 
				"device_imei", "device_mac", "device_no", "last_update_time", "apply_time", "apply_location_name", "apply_district_name", "description", 
				"remark", "device_tools_version_name", "device_tools_version_id", "login_counts"};
		return null;
	}
	
	private TerminalInfo setting(TerminalInfo terminalInfo, String columnName, ByteBuffer value){	
		try{
			TerminalInfoEnum column = TerminalInfoEnum.valueOf(columnName);
			switch(column){
				case device_id:
					terminalInfo.setDeviceId(stringSerializer.fromByteBuffer(value));
					break;
				case device_type_name:
					terminalInfo.setDeviceTypeName(stringSerializer.fromByteBuffer(value));
					break;
				case device_type_id:
					terminalInfo.setDeviceTypeId(stringSerializer.fromByteBuffer(value));
					break;
				case device_os_name:
					terminalInfo.setDeviceOsName(stringSerializer.fromByteBuffer(value));
					break;
				case device_os_id:
					terminalInfo.setDeviceOsId(stringSerializer.fromByteBuffer(value));
					break;
				case device_group_name:
					terminalInfo.setDeviceGroupName(stringSerializer.fromByteBuffer(value));
					break;
				case device_group_id:
					terminalInfo.setDeviceGroupId(stringSerializer.fromByteBuffer(value));
					break;
				case device_imei:
					terminalInfo.setDeviceImei(stringSerializer.fromByteBuffer(value));
					break;
				case device_mac:
					terminalInfo.setDeviceMac(stringSerializer.fromByteBuffer(value));
					break;
				case device_no:
					terminalInfo.setDeviceNo(stringSerializer.fromByteBuffer(value));
					break;
				case last_update_time:
					terminalInfo.setLastUpdateTime(longSerializer.fromByteBuffer(value));
					break;
				case apply_time:
					terminalInfo.setApplyTime(longSerializer.fromByteBuffer(value));
					break;
				case apply_location_name:
					terminalInfo.setApplyLocationName(stringSerializer.fromByteBuffer(value));
					break;
				case apply_district_name:
					terminalInfo.setApplyDistrictName(stringSerializer.fromByteBuffer(value));
					break;
				case description:
					terminalInfo.setDescription(stringSerializer.fromByteBuffer(value));
					break;
				case remark:
					terminalInfo.setRemark(stringSerializer.fromByteBuffer(value));
					break;
				case device_tools_version_name:
					terminalInfo.setDeviceToolVersionName(stringSerializer.fromByteBuffer(value));
					break;
				case device_tools_version_id:
					terminalInfo.setDeviceToolVersionId(stringSerializer.fromByteBuffer(value));
					break;
				case login_counts:
					terminalInfo.setLoginCounts(longSerializer.fromByteBuffer(value));
					break;
			}
		}catch(Exception e){
			logger.debug("Deleted Column:: "+columnName);
		}
		return terminalInfo;
	}
	
}
