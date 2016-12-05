package com.chinamobile.httpserver.response;

import java.util.HashMap;

import com.chinamobile.httpserver.response.file.RequestHtml;

public class HtmlRespFactory {

	CmriFileResponse getFileResponse(){
		return null;
	}
	
	private static final HashMap<String, String> DEFAULT_RESP_MAP = new HashMap<String, String>();

    static {
        // first populate the default command list
    	DEFAULT_RESP_MAP.put("requesthtml", RequestHtml.class.getName());
    }
    
    public static CmriFileResponse getResponse(String respName){
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