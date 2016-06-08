package com.chinamobile.httpserver.response.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.GlobalPara;
import com.chinamobile.httpserver.response.CmriFileResponse;

public class RequestZipReport  implements CmriFileResponse{

	private static RequestZipReport instance = null;
	private static String default_root_dict = GlobalPara.reportZipTemp;
	
	public static RequestZipReport getInstance() {
		if(instance == null){
			return new RequestZipReport();
		}
        return instance;
    }
	
	public RequestZipReport() {
		
	}
	
	@Override
	public HttpResponseMessage execute(Object message,String fileName) throws IOException {		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.setContentType("application/zip");
		try {
			File zipFile = new File(default_root_dict+System.getProperty("file.separator")+fileName+".zip");
			/*
			byte[] contents = null;
			if(zipFile.exists()){
				contents = RequestZipReport.toByteArray(zipFile.getAbsolutePath());
				response.appendBody(contents);
			}else{
				response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
				response.appendBody("File is not exist!"+"\n");
			}
			*/
			if(zipFile.exists()){
				response.setBodyFile(zipFile);
			}else{
				response.setResponseCode(HttpResponseMessage.HTTP_STATUS_NOT_FOUND);
				response.appendBody("File is not exist!"+"\n");
			}
//			File file = findFile(default_root_dict,fileName);
//			if(file == null){
//				return response;
//			}
//			BufferedImage image = ImageIO.read(new FileInputStream(file.getAbsolutePath()));
//			ByteArrayOutputStream baos = new ByteArrayOutputStream();  
//	        ImageIO.write(image, "png", baos);  
//			response.appendBody(baos.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
        }
		return response;
	}
//	
//	public File findFile(String rootDirectory, String fileName){
//		File[] list = (new File(rootDirectory)).listFiles();
//		for(int i=0; i<list.length; i++){
//			if(list[i].getName().trim().equals(fileName.trim())){
//				return list[i];
//			}
//		}
//		return null;
//	}
	
	 /** 
     * NIO way 
     * @param filename 
     * @return 
     * @throws IOException 
     */  
    public static byte[] toByteArray(String filename)throws IOException{  
          
        File f = new File(filename);  
        if(!f.exists()){  
            throw new FileNotFoundException(filename);  
        }  
          
        FileChannel channel = null;  
        FileInputStream fs = null;  
        try{  
            fs = new FileInputStream(f);  
            channel = fs.getChannel();  
            ByteBuffer byteBuffer = ByteBuffer.allocate((int)channel.size());  
            while((channel.read(byteBuffer)) > 0){  
                // do nothing  
//              System.out.println("reading");  
            }  
            return byteBuffer.array();  
        }catch (IOException e) {  
            e.printStackTrace();  
            throw e;  
        }finally{  
            try{  
                channel.close();  
            }catch (IOException e) {  
                e.printStackTrace();  
            }  
            try{  
                fs.close();  
            }catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
    } 

}
