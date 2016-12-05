package org.apache.mina.example.httpserver.uploadfactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.MinaHttpFileUpload;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;

import com.chinamobile.httpserver.data.GlobalPara;
import com.chinamobile.util.ZipUtil;

public class MultiPartProtocal {
	File[] body = null;

	static public boolean doParse(HttpRequestMessage request, Map<String,Object> list2insert) {
		// for(int i=0;i<buf.length-1;i++){
		// if(buf[i+1] == (byte) 0x0A && buf[i] == (byte) 0x0D){
		//
		// }
		// }
		String uploadPath = (String)list2insert.get("store_path_rela");
		String filesPath = "";
		File dstAdressFile = new File(uploadPath);
		if (!dstAdressFile.exists()) {
			dstAdressFile.mkdirs();
		}
		boolean isMultipart = MinaHttpFileUpload.isMultipartContent(request);
		if (isMultipart == true) {
			try {
				FileItemFactory factory = new DiskFileItemFactory();
				MinaHttpFileUpload upload = new MinaHttpFileUpload(factory);
				List<FileItem> items = upload.parseRequest(request);// 得到所有的文件
				Iterator<FileItem> itr = items.iterator();
				while (itr.hasNext()) {
					try{
						// 依次处理每个文件
						FileItem item = (FileItem) itr.next();
						String fileName = item.getName();// 获得文件名，包括路径
						if (fileName != null) {
							//存储至格式化的路径
							File fullFile = new File(fileName);
							File savedFile = new File(uploadPath,
									fullFile.getName());
							item.write(savedFile);
							if(fileName.toLowerCase().endsWith(".zip")){
								filesPath = ZipUtil.UnZIP(uploadPath+System.getProperty("file.separator")+fullFile.getName(), uploadPath+System.getProperty("file.separator"));
								try{
									if(savedFile.exists()){
										savedFile.delete();
									}
								}catch(Exception e){
									e.printStackTrace();
								}
								
							}
							
							//存储至备份路径
							String endWithStr = "";
							if((String)list2insert.get("test_mode")!=null && !"".equals((String)list2insert.get("test_mode"))){
								endWithStr = endWithStr.concat("-_-"+(String)list2insert.get("test_mode"));
							}else{
								endWithStr = endWithStr.concat("-_-"+"EMPTY");
							}
							if((String)list2insert.get("device_type")!=null && !"".equals((String)list2insert.get("device_type"))){
								endWithStr = endWithStr.concat("-_-"+(String)list2insert.get("device_type"));
							}else{
								endWithStr = endWithStr.concat("-_-"+"EMPTY");
							}
							if((String)list2insert.get("device_model")!=null && !"".equals((String)list2insert.get("device_model"))){
								endWithStr = endWithStr.concat("-_-"+(String)list2insert.get("device_model"));
							}else{
								endWithStr = endWithStr.concat("-_-"+"EMPTY");
							}
							if((String)list2insert.get("device_mac")!=null && !"".equals((String)list2insert.get("device_mac"))){
								endWithStr = endWithStr.concat("-_-"+(String)list2insert.get("device_mac"));
							}else{
								endWithStr = endWithStr.concat("-_-"+"EMPTY");
							}
							if((String)list2insert.get("device_imei")!=null && !"".equals((String)list2insert.get("device_imei"))){
								endWithStr = endWithStr.concat("-_-"+(String)list2insert.get("device_imei"));
							}else{
								endWithStr = endWithStr.concat("-_-"+"EMPTY");
							}
							
							File savedBackupFile = new File(GlobalPara.backupPath,
									fullFile.getName());
							item.write(savedBackupFile);							
							if(fileName.toLowerCase().endsWith(".zip")){
								ZipUtil.UnZIP(GlobalPara.backupPath+savedBackupFile.getName(), GlobalPara.backupPath, endWithStr);
								try{
									if(savedBackupFile.exists()){
										savedBackupFile.delete();
									}
								}catch(Exception e){
									e.printStackTrace();
								}
								
							}
							
						}
					}catch(Exception e) {
						e.printStackTrace();
					}
				}
				System.out.println("upload succeed");
				list2insert.put("store_path",filesPath);
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("the enctype must be multipart/form-data");
		}

		list2insert.put("store_path",filesPath);
		return false;
	}
}
