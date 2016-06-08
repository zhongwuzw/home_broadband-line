package cn.speed.jdbc.utils;

public class SearchValue {
	String jsonStr = "";
	
	String deviceType = "";
	String deviceMfr = "";
	String deviceModel = "";
	String deviceLocation = "";
	String deviceState = "";
	String deviceTestState = "";
	
	public SearchValue(){
	}
	
	public SearchValue(String jsonStr){
		this.jsonStr = jsonStr;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		if(deviceType!=null && !deviceType.toLowerCase().equals("all")){
			this.deviceType = deviceType;
		} else {
			this.deviceType = "";
		}
	}

	public String getDeviceMfr() {
		return deviceMfr;
	}

	public void setDeviceMfr(String deviceMfr) {
		if(deviceMfr!=null && !deviceMfr.toLowerCase().equals("all")){
			this.deviceMfr = deviceMfr;
		} else {
			this.deviceMfr = "";
		}
	}

	public String getDeviceModel() {
		return deviceModel;
	}

	public void setDeviceModel(String deviceModel) {
		if(deviceModel!=null && !deviceModel.toLowerCase().equals("all")){
			this.deviceModel = deviceModel;
		} else {
			this.deviceModel = "";
		}
	}

	public String getDeviceLocation() {
		return deviceLocation;
	}

	public void setDeviceLocation(String deviceLocation) {
		if(deviceLocation!=null && !deviceLocation.toLowerCase().equals("all")){
			this.deviceLocation = deviceLocation;
		} else {
			this.deviceLocation = "";
		}
	}

	public String getDeviceState() {
		return deviceState;
	}

	public void setDeviceState(String deviceState) {
		if(deviceState!=null && !deviceState.toLowerCase().equals("all")){
			this.deviceState = deviceState;
		} else {
			this.deviceState = "";
		}
	}

	public String getDeviceTestState() {
		return deviceTestState;
	}

	public void setDeviceTestState(String deviceTestState) {
		if(deviceTestState!=null && !deviceTestState.toLowerCase().equals("all")){
			this.deviceTestState = deviceTestState;
		} else {
			this.deviceTestState = "";
		}
	}
	
	public SearchValue parse(String[] contentStr){
		if(contentStr != null && contentStr.length==6){
			this.setDeviceType(contentStr[0]);
			this.setDeviceMfr(contentStr[1]);
			this.setDeviceModel(contentStr[2]);
			this.setDeviceLocation(contentStr[3]);
			this.setDeviceState(contentStr[4]);
			this.setDeviceTestState(contentStr[5]);
			return this;
		}
		return null;
	}
	
}
