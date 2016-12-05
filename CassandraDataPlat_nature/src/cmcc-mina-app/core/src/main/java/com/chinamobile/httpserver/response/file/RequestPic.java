package com.chinamobile.httpserver.response.file;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriFileResponse;

public class RequestPic  implements CmriFileResponse{

	private static RequestPic instance = null;
	private static String default_root_dict = "res/images";
	
	public static RequestPic getInstance() {
		if(instance == null){
			return new RequestPic();
		}
        return instance;
    }
	
	public RequestPic() {}
	
	@Override
	public HttpResponseMessage execute(Object message,String fileName) throws IOException {		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.setContentType("image/png");
		try {
			File picFile = new File(default_root_dict+System.getProperty("file.separator")+fileName);
			if(picFile.exists()){
				BufferedImage image = ImageIO.read(new FileInputStream(picFile));
				ByteArrayOutputStream baos = new ByteArrayOutputStream();  
		        ImageIO.write(image, "png", baos);  
				response.appendBody(baos.toByteArray());
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

}
