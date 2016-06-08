package com.chinamobile.httpserver.response;

import java.util.HashMap;

import com.chinamobile.datacollector.v2.ott.GetOttDataV2;
import com.chinamobile.datacollector.v2.ott.PostOttDataV2;
import com.chinamobile.datacollector.v2.ott.VerifyOttCacheV2;
import com.chinamobile.httpserver.response.impl.ClearMessageQueue;
import com.chinamobile.httpserver.response.impl.GetUeStatus;
import com.chinamobile.httpserver.response.impl.GoogleSearch;
import com.chinamobile.httpserver.response.impl.RequestFail;
import com.chinamobile.httpserver.response.impl.RequestSuccess;
import com.chinamobile.httpserver.response.impl.ResetUE;
import com.chinamobile.httpserver.response.impl.UploadDetail;
import com.chinamobile.httpserver.response.impl.UploadInfo;
import com.chinamobile.httpserver.response.impl.UploadLog;
import com.chinamobile.httpserver.response.impl.UploadPage;
import com.chinamobile.httpserver.response.js.Jquery_Min_JS;
import com.chinamobile.httpserver.response.js.Json2_JS;
import com.chinamobile.httpserver.response.js.Map_JS;
import com.chinamobile.httpserver.response.js.ShowDetail_JS;
import com.chinamobile.httpserver.response.js.ShowLog_JS;
import com.chinamobile.httpserver.response.xml.CarJsonMessage;
import com.chinamobile.httpserver.response.xml.GetOttData;
import com.chinamobile.httpserver.response.xml.OtsAndroidJsonMessage;
import com.chinamobile.httpserver.response.xml.PostJsonMessage;
import com.chinamobile.httpserver.response.xml.PostOttData;
import com.chinamobile.httpserver.response.xml.PostXmlMessage;
import com.chinamobile.httpserver.response.xml.PostXml;
import com.chinamobile.httpserver.response.xml.UeJsonMessage;
import com.chinamobile.httpserver.response.xml.VerifyOttCache;

public class HttpRespFactory {

	CmriHttpResponse getHttpResponse(){
		return null;
	}
	
	private static final HashMap<String, String> DEFAULT_RESP_MAP = new HashMap<String, String>();

    static {
        // first populate the default command list
    	DEFAULT_RESP_MAP.put("uploadinfo", UploadInfo.class.getName());
    	DEFAULT_RESP_MAP.put("uploaddetail", UploadDetail.class.getName());
    	DEFAULT_RESP_MAP.put("uploadlog", UploadLog.class.getName());
    	DEFAULT_RESP_MAP.put("requestsuccess", RequestSuccess.class.getName());
    	DEFAULT_RESP_MAP.put("requestfail", RequestFail.class.getName());
//    	DEFAULT_RESP_MAP.put("map.js", Map_JS.class.getName());
//    	DEFAULT_RESP_MAP.put("jquery-1.7.2.min.js", Jquery_Min_JS.class.getName());
//    	DEFAULT_RESP_MAP.put("json2.js", Json2_JS.class.getName());
//    	DEFAULT_RESP_MAP.put("showlog.js", ShowLog_JS.class.getName());
//    	DEFAULT_RESP_MAP.put("showdetail.js", ShowDetail_JS.class.getName());
//    	DEFAULT_RESP_MAP.put("uploadapply.action", PostXmlMessage.class.getName());
    	DEFAULT_RESP_MAP.put("postjsonmessage", PostJsonMessage.class.getName());
    	DEFAULT_RESP_MAP.put("carjsonmessage", CarJsonMessage.class.getName());
    	DEFAULT_RESP_MAP.put("uejsonmessage", UeJsonMessage.class.getName());
    	DEFAULT_RESP_MAP.put("postxml", PostXml.class.getName());
    	DEFAULT_RESP_MAP.put("googlesearch", GoogleSearch.class.getName());
    	DEFAULT_RESP_MAP.put("getuestatus", GetUeStatus.class.getName());
    	DEFAULT_RESP_MAP.put("resetue", ResetUE.class.getName());
    	DEFAULT_RESP_MAP.put("clearmessagequeue", ClearMessageQueue.class.getName());
    	
    	DEFAULT_RESP_MAP.put("uploadpage", UploadPage.class.getName());
    	DEFAULT_RESP_MAP.put("node00", OtsAndroidJsonMessage.class.getName());
    	

    	DEFAULT_RESP_MAP.put("postottdata", PostOttData.class.getName());
    	DEFAULT_RESP_MAP.put("getottdata", GetOttData.class.getName());
    	DEFAULT_RESP_MAP.put("verifyottcache", VerifyOttCache.class.getName());
    	
    	DEFAULT_RESP_MAP.put("postottdatav2", PostOttDataV2.class.getName());
    	DEFAULT_RESP_MAP.put("getottdatav2", GetOttDataV2.class.getName());
    	DEFAULT_RESP_MAP.put("verifyottcachev2", VerifyOttCacheV2.class.getName());
    	
    }
    
    public static CmriHttpResponse getResponse(String respName){
    	
        String lowerCaseRespName = respName.toLowerCase().replaceAll("/","");
        
        /*
        if (lowerCaseRespName != null && !lowerCaseRespName.equals("") && lowerCaseRespName.contains("postottdata")) {
    		respName = "postottdata";
        }else 
        */
        if (lowerCaseRespName == null || lowerCaseRespName.equals("") || DEFAULT_RESP_MAP.get(lowerCaseRespName) == null) {
    		respName = "requestfail";
        }else{
        	respName = lowerCaseRespName;
        }
    	System.out.println("respName--------------"+respName);
    	Class<?> responseClass=null;
        try{
       	 responseClass=Class.forName(DEFAULT_RESP_MAP.get(respName));
        }catch (Exception e) {
            e.printStackTrace();
        }
        CmriHttpResponse myCmriHttpResponse=null;
        try {
        	myCmriHttpResponse=(CmriHttpResponse)responseClass.newInstance();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    	return myCmriHttpResponse;
    }
}
