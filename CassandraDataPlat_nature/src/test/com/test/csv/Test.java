package com.test.csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.Date;

import net.sf.json.JSONObject;

import com.csvreader.CsvWriter;

public class Test {

	private static String srcPath = "/opt/tomcat-saga02/webapps/ctp/terminal/heartbeattrace";
	private String destPath = "/ctp";
	int count = 0;
//	public static void main(String[] args) {
//		String destFile = "/storng/emulate/1.2.1/temp/10M.dat";
//		System.out.println(destFile.lastIndexOf("/"));
//		System.out.println(destFile.indexOf("."));
//		System.out.println(destFile.substring(destFile.lastIndexOf("/")+1, destFile.lastIndexOf(".")));
//	}
	public static void main(String[] args){
		try {
			File file = new File(srcPath);
			Test test = new Test();
			test.readAll(file.getAbsolutePath());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//读取目标路径的文件
	public void readAll(String src){
		Date start = new Date();
		System.out.println("Start:"+start.toLocaleString());
		File file = new File(src);
		if(!file.exists()){
			file.mkdirs();
		}
		String [] str = new String[4];
		if(file.isFile() && file.getName().endsWith(".log")){
			String dataStr = "";
			str = read(file.getAbsolutePath(),"utf-8");
			for (int i = 0; i < str.length; i++) {
				if(i==str.length-1){
					dataStr = dataStr + str[i]+ "" ;
				}else{
					dataStr = dataStr + str[i] +",";					
				}
			}
			write(dataStr);
		}else
		{
			File[] fileList = file.listFiles();
			for (int i = 0; i < fileList.length; i++) {
				File sonFile = fileList[i];
				if(sonFile.isDirectory()){
					readAll(sonFile.getAbsolutePath());
				}else{
					if(!sonFile.getName().endsWith(".log")){
						continue;
					}
					System.out.println("正在读取 "+sonFile.getParent()+" 文件夹中第"+(i+1)+"个文件，共"+fileList.length+"个文件");
					String dataStr = "";
					str = read(sonFile.getAbsolutePath(),"utf-8");
					for (int j = 0; j < str.length; j++) {
						if(j==str.length-1){
							dataStr = dataStr + str[j]+ "" ;
						}else{
							dataStr = dataStr + str[j] +",";					
						}
					}
					write(dataStr);
				}
			}
		}
		Date end = new Date();
		System.out.println("Start:"+start.toLocaleString()+">>> End:"+end.toLocaleString());
	}
	 /** 
     *  读取文件
     * @param filename 目标文件 
     */  
    public String[] read(String filename,String charset) {
    	count++;
    	if(count%1000==0){
			System.out.println("Dealed With "+count+" Datas");
		}
    	JSONObject json = new JSONObject();
        String jsonStr = "";
        String longitude = "";
        String latitude = "";
        File file = new File(filename);
        String imei = "";
    	imei = file.getName().substring(0, file.getName().indexOf(".log"));	
        
        String gps = "";
        String result[] = new String[4];
        RandomAccessFile rf = null;  
        try {
            rf = new RandomAccessFile(filename, "r");  
            long len = rf.length();  
            long start = rf.getFilePointer();  
            long nextend = start + len - 1;  
            String line = "";  
            rf.seek(nextend);  
            int c = -1;  
            while (nextend > start) {
                c = rf.read();  
                if (c == '\n' || c == '\r') {  
                    line = rf.readLine();  
                    if (line != null) {  
                        line = new String(line.getBytes("ISO-8859-1"), charset);
                    }
                    nextend--;  
                }
                nextend--;  
                rf.seek(nextend);  
                if (nextend == 0) {// 当文件指针退至文件开始处，输出第一行  
                	line = rf.readLine();
                    //System.out.println(line);//為空白一行
                    line = rf.readLine();
                    line = new String(line.getBytes("ISO-8859-1"), charset);
                }
                if(line!=null && !line.equals("")){
                	jsonStr = line.toString().substring(line.indexOf("{"),line.indexOf("}")+1);
                }
                if(jsonStr.equals("")){
                	continue;
                }
                json = JSONObject.fromObject(jsonStr);
                if(json.containsKey("latitude")){
                	latitude = json.getString("latitude");
                }
                if(json.containsKey("longitude")){
                	longitude = json.getString("longitude");
                }
                if(latitude!=null && !latitude.equals("") && longitude!=null && !longitude.equals("")){
                	break;
                }
            }
            gps = longitude +" "+ latitude;
            result[0] = gps;
            if(longitude.equals("")){
            	result[1] = longitude;
            }else{
            	result[1] = transGpsPoint(longitude)[0];
            }
            if(latitude.equals("")){
            	result[2] = latitude;
            }else{
            	result[2] = transGpsPoint(latitude)[1];
            }
            result[3] = imei;
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        } finally { 
            try {  
                if (rf != null)  
                    rf.close();  
            } catch (IOException e) {  
                e.printStackTrace();  
            }
            return result;
        }  
    }
    /**
	 * GPS坐标转换
	 * @param meta
	 * @return
	 */
	private String[] transGpsPoint(String meta){

		String[] result = new String[2];
		String[] info = null;
		if(meta!=null && !"".equals(meta)){
			info = meta.split(" ");
		}else{
			return null;
		}
		
		String latitude = "";
		String longitude = "";
		
		String gpsPoint = "";
		
		for(int i=0; i<info.length&&i<2; i++){
			if(info[i].contains("°")){
				String degrees = info[i].substring(0,info[i].lastIndexOf("°"));
				String minutes = info[i].substring(info[i].lastIndexOf("°")+1,info[i].lastIndexOf("′"));
				String seconds = info[i].substring(info[i].lastIndexOf("′")+1,info[i].lastIndexOf("″"));
				
				//Long gpsLong = Long.parseLong(degrees)+Long.parseLong(minutes)/60+Long.parseLong(seconds)/3600;
				float gpsLong = Float.parseFloat(degrees)+Float.parseFloat(minutes)/60+Float.parseFloat(seconds)/3600;
				
				DecimalFormat decimalFormat=new DecimalFormat(".0000000");
				gpsPoint = decimalFormat.format(gpsLong);
				if(info[i].contains("E")){
					longitude = gpsPoint;
				}else if(info[i].contains("N")){
					latitude = gpsPoint;
				}else if(gpsLong>80.0){
					longitude = gpsPoint;
				}else{
					latitude = gpsPoint;
				}
			}else{
				if(info[i].contains("E")){
					gpsPoint = info[i].substring(0,info[i].lastIndexOf("E"));
					float gpsLong = Float.valueOf(gpsPoint);
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					longitude = gpsPoint;
				}else if(info[i].contains("N")){
					gpsPoint = info[i].substring(0,info[i].lastIndexOf("N"));
					float gpsLong = Float.valueOf(gpsPoint);
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					latitude = gpsPoint;
				}else{
					gpsPoint = info[i];
					float gpsLong = Float.valueOf(gpsPoint);
					DecimalFormat decimalFormat=new DecimalFormat(".0000000");
					gpsPoint = decimalFormat.format(gpsLong);
					if(gpsLong>80.0){
						longitude = gpsPoint;
					}else{
						latitude = gpsPoint;
					}
				}
			}
		}
		result[0] = longitude;
		result[1] = latitude;
		return result;
	}
	
	public void write(String dataStr){
		File csvFile = new File(destPath+File.separator+"terminalPositionTest.csv");
		BufferedWriter writer = null;
		CsvWriter cwriter = null;
		try {
			if(!csvFile.exists()){
				csvFile.getParentFile().mkdirs();
				csvFile.createNewFile();
			}
			writer = new BufferedWriter(new FileWriter(csvFile,true));
			cwriter = new CsvWriter(writer,',');
		} catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
            cwriter.writeRecord(dataStr.split(","), true);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(writer!=null){
				try {
					writer.flush();
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(cwriter!=null){
				cwriter.flush();
				cwriter.close();
			}
		}
	}
}
