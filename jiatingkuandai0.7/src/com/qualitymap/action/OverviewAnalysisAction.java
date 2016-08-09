package com.qualitymap.action;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import sun.misc.BASE64Decoder;

import com.alibaba.fastjson.JSONObject;
import com.lowagie.text.Document;
import com.lowagie.text.DocumentException;
import com.lowagie.text.Image;
import com.lowagie.text.Paragraph;
import com.lowagie.text.pdf.PdfWriter;
import com.qualitymap.base.BaseAction;

public class OverviewAnalysisAction extends BaseAction {
	
	private static String GenerateImage(String imgStr){
		//对字节数组字符串进行Base64解码并生成图片
		String imgFilePath=null;
        if (imgStr == null) //图像数据为空
            return imgFilePath;
        BASE64Decoder decoder = new BASE64Decoder();
        try 
        {
            //Base64解码
            byte[] b = decoder.decodeBuffer(imgStr);
            for(int i=0;i<b.length;++i)
            {
                if(b[i]<0)
                {//调整异常数据
                    b[i]+=256;
                }
            }
            //生成jpeg图片
            //String pathString=(new File("")).getAbsolutePath().replaceAll("\\\\", "/");
    		//pathString=pathString.substring(0, pathString.length()-3);
    		
            imgFilePath = "../webapps/jiatingkuandai/222.jpg";//新生成的图片
            OutputStream out = new FileOutputStream(imgFilePath);    
            out.write(b);
            out.flush();
            out.close();
            return imgFilePath;
        } 
        catch (Exception e) 
        {
            return imgFilePath;
        }
    }
	/**
	 * 
	 * Description:
	 * Date:Nov 27, 2014
	 * @author HuangLongJian 
	 * @return void
	 */
	public void creatPdfImport(){
	    String dataUrl=servletRequest.getParameter("dataUrl");
	    dataUrl=dataUrl.substring(22, dataUrl.length());
	    String fileName=servletRequest.getParameter("fileName");
		//System.out.println(dataUrl);
		String imgFilePath=GenerateImage(dataUrl);
		//d:\222.jpg
		String outJsonStr = "{\"filestatus\":\"o\"}";
		//String serverPath = FTPClientUtil.getAbsoluteServerPath(servletRequest);
	   if(imgFilePath!=null){
		   //imgFilePath=imgFilePath.replace('\\', '/');
		   System.out.println(imgFilePath);
		   try {
			   Document doc=new Document();
	    	  //定义输出位置并把文档对象装入输出对象中
	        	PdfWriter.getInstance(doc, new FileOutputStream("../webapps/jiatingkuandai/"+fileName));
	        	//PdfWriter.
	           //打开文档对象
	           doc.open();
	           doc.add(new Paragraph("预览"));
	           Image jpeg = null;
		  			try {
		  				jpeg = Image.getInstance(imgFilePath);
		  			} catch (MalformedURLException e) {
		  				outJsonStr = "{\"filestatus\":\"jpgnotfound\"}";
		  				e.printStackTrace();
		  			} catch (IOException e) {
		  				e.printStackTrace();
		  			}
		  			jpeg.setAlignment(Image.ALIGN_CENTER);
		  			jpeg.scalePercent(30);
		  			//jpeg.set
			        doc.add(jpeg);
			         //  关闭文档对象，释放资源 
			        doc.close(); 
			        outJsonStr = "{\"filestatus\":\"ok\"}";
		    } catch (FileNotFoundException e) {
		       outJsonStr = "{\"filestatus\":\"FileNotFound\"}";
	           e.printStackTrace();
		    } catch (DocumentException e) {
	           e.printStackTrace();
	       }
	   }
	   printWriter(outJsonStr);
	}
	/**
	 * 测试报告中的用例数获取
	 * date:2016.07.04
	 * @author zhaoqh
	 */
	public void getCaseCount(){
		String casecountUrl = "";
		Properties properties = new Properties();
		InputStream in = OverviewAnalysisAction.class.getClassLoader().getResourceAsStream("config/common.properties");
		try{
			properties.load(in);
			//获取用例数的地址
			casecountUrl = properties.getProperty("getCasecountUrl");
		}catch(IOException e){
			e.printStackTrace();
		}
		String jsonCasecount = otspost(null, casecountUrl);
		System.out.print("测试报告获取用例数json："+jsonCasecount);
		printWriter(jsonCasecount);
	}
	/**
	 * 根据url获取返回json
	 * @param param
	 * @param url
	 * @return
	 */
	public static String otspost(JSONObject param,String url){
		PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            URLConnection conn = realUrl.openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            out = new PrintWriter(conn.getOutputStream());
            out.print(param);
            out.flush();
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(),"utf-8"));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }
        return result;
    }
	
	public static  void main(String[]  arges){
		
	}
}
