package com.chinamobile.httpserver.response;

import java.util.HashMap;

import cn.speed.history.action.FindDeviceAction;
import cn.speed.history.action.RefreshDeviceAction;

import com.chinamobile.httpserver.response.action.GetDataInfo;
import com.chinamobile.httpserver.response.action.GetGpsAreaTestData;
import com.chinamobile.httpserver.response.action.UpdateDataInfo;
import com.chinamobile.httpserver.response.xml.GetUserInfo;
import com.chinamobile.httpserver.response.xml.PostXmlMessage;

public class ActionRespFactory {

	CmriJsonResponse getJsonResponse(){
		return null;
	}
	
	private static final HashMap<String, String> DEFAULT_RESP_MAP = new HashMap<String, String>();

    static {
        // first populate the default command list
    	DEFAULT_RESP_MAP.put("ueaddressdetect.action", GetUserInfo.class.getName());
    	DEFAULT_RESP_MAP.put("uploadapply.action", PostXmlMessage.class.getName());
    	DEFAULT_RESP_MAP.put("getdatainfo.action", GetDataInfo.class.getName());
    	DEFAULT_RESP_MAP.put("insertdatainfo.action", UpdateDataInfo.class.getName());
    	DEFAULT_RESP_MAP.put("finddevice.action", FindDeviceAction.class.getName());
    	DEFAULT_RESP_MAP.put("refreshdevice.action", RefreshDeviceAction.class.getName());
    	DEFAULT_RESP_MAP.put("getgpsareatestdata.action", GetGpsAreaTestData.class.getName());
    }
    
    public static CmriJsonResponse getResponse(String respName){
        String lowerCaseRespName = respName.toLowerCase().replaceAll("/","");
        
    	if (lowerCaseRespName == null || lowerCaseRespName.equals("") || DEFAULT_RESP_MAP.get(lowerCaseRespName) == null) {
    		respName = "requestfail";
        }else{
        	respName = lowerCaseRespName;
        }
    	System.out.println("lowerCaseRespName——————————————————"+lowerCaseRespName);
    	Class<?> responseClass=null;
        try{
       	 responseClass=Class.forName(DEFAULT_RESP_MAP.get(respName));
        }catch (Exception e) {
            e.printStackTrace();
        }
        CmriJsonResponse myCmriJsonResponse=null;
        try {
        	myCmriJsonResponse=(CmriJsonResponse)responseClass.newInstance();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    	return myCmriJsonResponse;
    }
}
