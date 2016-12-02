package com.chinamobile.httpserver.response;

import java.util.HashMap;

import com.chinamobile.httpserver.response.file.LteDataUpload;

public class UploadRespFactory {

	CmriUploadResponse getFileResponse(){
		return null;
	}
	
	private static final HashMap<String, String> DEFAULT_RESP_MAP = new HashMap<String, String>();

    static {
        // first populate the default command list
    	DEFAULT_RESP_MAP.put("uploadfile.upload", LteDataUpload.class.getName());
    }
    
    public static CmriUploadResponse getResponse(String respName){
    	
    	 Class<?> responseClass=null;
         try{
        	 responseClass=Class.forName(LteDataUpload.class.getName());
         }catch (Exception e) {
             e.printStackTrace();
         }
         CmriUploadResponse myCmriFileResponse=null;
         try {
        	 myCmriFileResponse=(CmriUploadResponse)responseClass.newInstance();
         } catch (InstantiationException e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
         } catch (IllegalAccessException e) {
             // TODO Auto-generated catch block
             e.printStackTrace();
         }
    	return myCmriFileResponse;
    }
}