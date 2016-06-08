package com.chinamobile.httpserver.response;

import java.util.HashMap;

import com.chinamobile.httpserver.response.action.GetTestDataByCassandra;
import com.chinamobile.httpserver.response.action.GetTestDataByCtp;

public class CassandraRespFactory {

	CmriFileResponse getFileResponse(){
		return null;
	}
	
	private static final HashMap<String, String> DEFAULT_RESP_MAP = new HashMap<String, String>();

    static {
        // first populate the default command list
    	DEFAULT_RESP_MAP.put("queryTestData", GetTestDataByCassandra.class.getName());
    	DEFAULT_RESP_MAP.put("queryCassandra", GetTestDataByCtp.class.getName());
    }
    
    public static CmriJsonResponse getResponse(String respName){

    	 Class<?> responseClass=null;
         try{
        	 if(respName.startsWith("queryTestData")){
        		 responseClass=Class.forName(DEFAULT_RESP_MAP.get("queryTestData"));
        	 }else if(respName.startsWith("queryCassandra")){
        		 responseClass=Class.forName(DEFAULT_RESP_MAP.get("queryCassandra"));
        	 }else{
        		 responseClass=Class.forName(DEFAULT_RESP_MAP.get(respName));
        	 }
         }catch (Exception e) {
             e.printStackTrace();
         }
         CmriJsonResponse myCmriFileResponse=null;
         try {
        	 myCmriFileResponse=(CmriJsonResponse)responseClass.newInstance();
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