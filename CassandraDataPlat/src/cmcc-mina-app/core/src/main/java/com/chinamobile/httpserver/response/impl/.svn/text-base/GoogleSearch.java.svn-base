package com.chinamobile.httpserver.response.impl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import com.chinamobile.httpserver.response.CmriHttpResponse;

public class GoogleSearch implements CmriHttpResponse{

	private static GoogleSearch instance = null;
	
	public static GoogleSearch getInstance() {
		if(instance == null){
			return new GoogleSearch();
		}
        return instance;
    }
	
	private GoogleSearch() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {	
		String content = ((HttpRequestMessage)message).getBody().trim();
		String value = content.split("=")[1];

		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.setContentType("text/xml; charset=utf-8");
		InputStream fis = null; 
		BufferedReader br = null;
		try{
			String path = GoogleSearch.class.getResource("").toURI().getPath();
			System.out.println("path="+path);
			path = path.substring(0,path.lastIndexOf("bin"));
			String confPath = path+File.separator+"res"+File.separator+"conf"+File.separator+"httpserver"+File.separator+"knowlege.properties";
			System.out.println("conf文件："+confPath);
			File confFile = new File(confPath);
			Properties pro = new Properties();
			pro.load(new FileInputStream(confFile));
			String url = pro.getProperty("url");
			System.out.println("url地址为："+url);
			
		
			
			HttpClient client = new DefaultHttpClient();//定义一个发送请求的http客户端
			
			HttpGet httpget = new HttpGet();
			httpget.setURI(new URI(url+value));
			HttpResponse responseFir = client.execute(httpget); 
//			Header h = responseFir.getEntity().getContentEncoding();
//			
//			HeaderElement[] he =  h.getElements();
//			for(int i=0; i<he.length; i++){
//				he[i].toString();
//			}
			fis = responseFir.getEntity().getContent();
			
//			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
//			
//			byte[] data = new byte[4096];
//			int count = -1;
//			while((count = fis.read(data, 0, 4096))!=-1)
//			{
//				outStream.write(data,0,count);
//			}
//			
//			
//			String line = new String(outStream.toByteArray(),"utf-8");
//			
//			
//			response.appendBody(line);
//			System.out.println(line);
			
			
			
			SAXBuilder builder = new SAXBuilder();
			Document doc = null;
		    try {
		        doc = builder.build(new InputStreamReader(fis,"UTF-8"));
		        Element rootEl = doc.getRootElement();
		        
		        
		        //获得所有子元素
		        List<Element> list = rootEl.getChildren();
		        for (Element el : list) {
		            String str = el.getChildText("url");
		            File resfile = new File(str);
		            if(resfile.exists()){
		            	Element el_fileSize = new Element("filesize");
	                    el_fileSize.addContent(FormetFileSize(resfile.length()));
		            	el.addContent(el_fileSize);
		            	
	                    Element el_fileDate = new Element("filedate");
	                    Date date=new Date(resfile.lastModified());
	                    SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	                    String lmd=sdf.format(date);
	                    el_fileDate.addContent(lmd);
		            	el.addContent(el_fileDate);
		            }
		        }
		    } catch (JDOMException e) {
		        e.printStackTrace();
		    } catch (IOException e) {
		        e.printStackTrace();
		    }
			
		    XMLOutputter XMLOut = new XMLOutputter(FormatXML());
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			
			try {
				XMLOut.output(doc,bo);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		    br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bo.toByteArray()),"UTF-8"));
			String line = null;
			while((line = br.readLine())!=null)
			{
				line = line.replaceAll("\\\\", "/");
				response.appendBody(line);
				response.appendBody("\n\r");
				System.out.println(line);
			}
			br.close();
			fis.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		System.out.println("请求搜索的消息："+value);
		
		return response;
	}
	
	static public Format FormatXML() {
		// 格式化生成的xml文件，如果不进行格式化的话，生成的xml文件将会是很长的一行...
		Format format = Format.getCompactFormat();
		format.setEncoding("utf-8");
		format.setIndent(" ");
		return format;
	}
	
	public String FormetFileSize(long fileS) {//转换文件大小
        DecimalFormat df = new DecimalFormat("#.00");
        String fileSizeString = "";
        if (fileS < 1024) {
            fileSizeString = df.format((double) fileS) + "B";
        } else if (fileS < 1048576) {
            fileSizeString = df.format((double) fileS / 1024) + "KB";
        } else if (fileS < 1073741824) {
            fileSizeString = df.format((double) fileS / 1048576) + "MB";
        } else {
            fileSizeString = df.format((double) fileS / 1073741824) + "GB";
        }
        return fileSizeString;
    }

}