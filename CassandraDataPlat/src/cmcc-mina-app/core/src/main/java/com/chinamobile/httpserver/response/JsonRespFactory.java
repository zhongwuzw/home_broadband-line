package com.chinamobile.httpserver.response;

import java.util.HashMap;

import com.chinamobile.httpserver.response.js.CarMsgDetail;
import com.chinamobile.httpserver.response.js.CarMsgShow;
import com.chinamobile.httpserver.response.js.RequestFail;
import com.chinamobile.httpserver.response.js.TestMessage;
import com.chinamobile.httpserver.response.js.UeMsgDetail;
import com.chinamobile.httpserver.response.js.UeMsgShow;

public class JsonRespFactory {

	CmriJsonResponse getJsonResponse(){
		return null;
	}
	
	private static final HashMap<String, String> DEFAULT_RESP_MAP = new HashMap<String, String>();

    static {
        // first populate the default command list
    	DEFAULT_RESP_MAP.put("testmessage.json", TestMessage.class.getName());
    	DEFAULT_RESP_MAP.put("uemsgdetail.json", UeMsgDetail.class.getName());
    	DEFAULT_RESP_MAP.put("carmsgdetail.json", CarMsgDetail.class.getName());
    	DEFAULT_RESP_MAP.put("uemsgshow.json", UeMsgShow.class.getName());
    	DEFAULT_RESP_MAP.put("carmsgshow.json", CarMsgShow.class.getName());
    	DEFAULT_RESP_MAP.put("requestfail", RequestFail.class.getName());
    }
    
    public static CmriJsonResponse getResponse(String respName){
    	/*
    	 * ��ǰֻ֧�ֽ���һ��Ŀ¼��URL���� http://localhost:8080/myweb/
    	 * ��֧�� http://localhost:8080/myweb/myinfo
    	 */
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
//        return DEFAULT_RESP_MAP.get(lowerCaseRespName);
    }
}
