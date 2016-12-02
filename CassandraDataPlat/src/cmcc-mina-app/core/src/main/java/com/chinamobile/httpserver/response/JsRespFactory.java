package com.chinamobile.httpserver.response;

import java.util.HashMap;

import com.chinamobile.httpserver.response.file.RequestJs;
import com.chinamobile.httpserver.response.file.RequestPic;

public class JsRespFactory {

	CmriFileResponse getFileResponse(){
		return null;
	}
	
	private static final HashMap<String, String> DEFAULT_RESP_MAP = new HashMap<String, String>();

    static {
        // first populate the default command list
    	DEFAULT_RESP_MAP.put("requestjs", RequestJs.class.getName());
    }
    
    public static CmriFileResponse getResponse(String respName){
    	/*
    	 * ��ǰֻ֧�ֽ���һ��Ŀ¼��URL���� http://localhost:8080/myweb/
    	 * ��֧�� http://localhost:8080/myweb/myinfo
    	 */
//        String lowerCaseRespName = respName.toLowerCase().replaceAll("/","");
//        
//    	if (lowerCaseRespName == null || lowerCaseRespName.equals("") || DEFAULT_RESP_MAP.get(lowerCaseRespName) == null) {
//            return DEFAULT_RESP_MAP.get("requestfail");
//        }
    	
    	 Class<?> responseClass=null;
         try{
        	 responseClass=Class.forName(DEFAULT_RESP_MAP.get(respName));
         }catch (Exception e) {
             e.printStackTrace();
         }
         CmriFileResponse myCmriFileResponse=null;
         try {
        	 myCmriFileResponse=(CmriFileResponse)responseClass.newInstance();
         } catch (InstantiationException e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
         } catch (IllegalAccessException e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
         }
    	return myCmriFileResponse;
//        return DEFAULT_RESP_MAP.get(respName);
    }
}