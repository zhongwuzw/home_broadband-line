package com.qualitymap.action;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import javax.servlet.ServletOutputStream;

import org.xhtmlrenderer.pdf.ITextFontResolver;
import org.xhtmlrenderer.pdf.ITextRenderer;

import sun.misc.BASE64Decoder;

import com.alibaba.fastjson.JSONObject;
import com.lowagie.text.DocumentException;
import com.lowagie.text.pdf.BaseFont;
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
    		
            imgFilePath = "../webapps/quality_map/pdf/222.jpg";//新生成的图片
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
	 * Date:Aug 8, 2016
	 * @author zqh 
	 * @return void
	 */
	public void creatPdfImport() throws UnsupportedEncodingException{
		
		String dataUrl = servletRequest.getParameter("dataUrl");
        dataUrl = dataUrl.substring(22, dataUrl.length());
        String pinYinStr = servletRequest.getParameter("pinYinStr");
        String htmlStr = servletRequest.getParameter("htmlStr");
        htmlStr = htmlStr.replaceAll("<br>", "<br />");
        
        String outJsonStr = "{\"filestatus\":\"o\"}";
        StringBuffer sb = new StringBuffer();
        sb.append("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">")
        .append("<html xmlns=\"http://www.w3.org/1999/xhtml\">")
        .append("<head>")
        .append("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=gbk\" />")
        .append("<style type='text/css' >")
        .append("body{font-family:'SimSun';line-height: 1.42857143;}")
        .append(".table{width: 100%;max-width: 100%;margin-bottom: 20px;}")
        //b, strong
        .append(".row {margin-right: -15px;}")
        /*.append(".col-sm-4{width: 33.33333333%;float: left;padding-left: 15px;}")
        .append(".row:after{clear: both;}")
        .append(".row:before{display: table;}")
        .append("address {margin-bottom: 20px;font-style: normal;line-height: 1.42857143;}")*/
        .append("strong {font-weight: 700;}")
        .append(".table>tbody>tr>td,.table>thead>tr>th {padding: 8px; vertical-align: top;}")
        .append("th {    text-align: left;}")
        .append(".table-striped>tbody>tr:nth-of-type(odd){background-color: #f9f9f9;}")
        /*.append(".row:after{clear: both;}")
        .append(".row:after{clear: both;}")
        .append(".row:after{clear: both;}")*/
        
        
        
        .append("</style>")
        .append("</head>")
        .append("<body>");
        sb.append(htmlStr)
        .append("</body>");
        sb.append("</html>");
        System.out.println("html页面：："+sb.toString());
        OutputStream out = null;
        try
        {
            out = new FileOutputStream("../webapps/quality_map/pdf/"+pinYinStr+".pdf");
        }
        catch(FileNotFoundException e)
        {
            e.printStackTrace();
        }
        ITextRenderer renderer = new ITextRenderer();
        ITextFontResolver fontResolver = renderer.getFontResolver();
        try
        {
            //fontResolver.addFont("C:/Windows/Fonts/SimSun.ttc", BaseFont.IDENTITY_H, BaseFont.NOT_EMBEDDED);
            fontResolver.addFont("../webapps/quality_map/pdf/simsun.ttc", BaseFont.IDENTITY_H, BaseFont.NOT_EMBEDDED);
        }
        catch(DocumentException e)
        {
            e.printStackTrace();
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        renderer.setDocumentFromString(sb.toString());
        renderer.layout();
        try
        {
            renderer.createPDF(out);
            outJsonStr = "{\"filestatus\":\"ok\"}";
            System.out.println("生成成功");
        }
        catch(DocumentException e)
        {
            e.printStackTrace();
        }
        try
        {
            out.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
		
		
	    /*String dataUrl=servletRequest.getParameter("dataUrl");
	    dataUrl=dataUrl.substring(22, dataUrl.length());
	    //String fileName=servletRequest.getParameter("fileName");
	    String pinYinStr = servletRequest.getParameter("pinYinStr");
		String imgFilePath=GenerateImage(dataUrl);
		String outJsonStr = "{\"filestatus\":\"o\"}";
	   if(imgFilePath!=null){
		   System.out.println(imgFilePath);
		   try {
			   
	           
	           //doc.add(new Paragraph("预览")); //一页
	           Image jpeg = null;
	  			try {
	  				jpeg = Image.getInstance(imgFilePath);
	  			} catch (MalformedURLException e) {
	  				outJsonStr = "{\"filestatus\":\"jpgnotfound\"}";
	  				e.printStackTrace();
	  			} catch (IOException e) {
	  				e.printStackTrace();
	  			}
	  			Rectangle pageSize = new Rectangle(jpeg.getPlainWidth()+100,jpeg.getPlainHeight());
				   Document doc=new Document(pageSize);
		    	  //定义输出位置并把文档对象装入输出对象中
		        	PdfWriter.getInstance(doc, new FileOutputStream("../webapps/jiatingkuandai/pdf/"+pinYinStr+".pdf"));
		        	//PdfWriter.
		           //打开文档对象
		           doc.open();
	  			float heigth = jpeg.getHeight(); 
	  			float width = jpeg.getWidth(); 
	  			int percent = getPercent2(heigth, width); 
	  			jpeg.setAlignment(Image.MIDDLE); 
	  			jpeg.scalePercent(percent+3);
	  			
	  			jpeg.setAlignment(Image.ALIGN_CENTER);
	  			jpeg.scalePercent(15);
		           jpeg.setAlignment(Image.ALIGN_CENTER);
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
	   }*/
	   printWriter(outJsonStr);
	}
	
	/** * 第二种解决方案，统一按照宽度压缩 这样来的效果是，所有图片的宽度是相等的，自我认为给客户的效果是最好的 * * @param args */ 
	public static int getPercent2(float h, float w) 
	{ 
		int p = 0; 
		float p2 = 0.0f; 
		p2 = 530 / h * 100; 
		p = Math.round(p2); 
		return p; 
	}
	
	/**
	 * 下载pdf
	 * @author zqh 2016-8-8 17:20
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public void downloadPDF() throws IOException   {

		InputStream is = null;
	    OutputStreamWriter reqOut = null;
	    String fileName = this.servletRequest.getParameter("fileName");
	    fileName = new String(fileName.getBytes("iso-8859-1"),"UTF-8");
	    String pinYinStr = servletRequest.getParameter("pinYinStr");
	    String path = fileName;
	    String filePath = "";
	    Properties properties = new Properties();
		InputStream in = OverviewAnalysisAction.class.getClassLoader().getResourceAsStream("config/download.properties");
		
		try{
			properties.load(in);
			
			filePath = properties.getProperty("downloadFilePath")+pinYinStr+".pdf";
		}catch(IOException e){
			e.printStackTrace();
		}
	    
	    
	    try
	    {
	      ServletOutputStream outputStream = null;   
	        File file = new File(path);
	          
	        String downloadName = new String(file.getName().getBytes("UTF-8"), "iso8859-1");

	        this.servletResponse.setContentType("application/octet-stream");

	        this.servletResponse.setHeader("Content-Disposition", "attachment; filename=\"" + downloadName + "\"");

	        outputStream = this.servletResponse.getOutputStream();
	        //filePath = "http://192.168.25.87:8080/jiatingkuandai/pdf/"+pinYinStr+".pdf";
	        //filePath = "http://192.168.39.107:8484/jiatingkuandai/pdf/"+pinYinStr+".pdf";

	        System.out.println("文件名为："+filePath);
	        URL reqUrl = new URL(filePath);

	        URLConnection connection = reqUrl.openConnection();
	        connection.setDoOutput(true);
	        connection.setRequestProperty("referer", "localhost");
	        reqOut = new OutputStreamWriter(connection.getOutputStream());
	        reqOut.write("path=" + path);
	        reqOut.flush();

	        is = connection.getInputStream();

	        byte[] contents = new byte[1024];
	        int len = 0;
	        while ((len=is.read(contents)) != -1) {
	          outputStream.write(contents,0,len);
	          System.out.println(contents.toString());
	          outputStream.flush();
	        }
	        outputStream.close();
	     } catch (Exception e) {
	        System.out.println("下载文件失败：" + e.getMessage());
	        e.printStackTrace();    
	      }finally
	      {
	        if (is != null) {
	          try {
	            is.close();
	          } catch (IOException e) {
	            e.printStackTrace();
	          }
	        }
	        if (reqOut != null){
	          try {
	            reqOut.close();
	          } catch (IOException e) {
	            e.printStackTrace();
	          }
	      }
	      
	    }
		
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
	
	/*public static String getPingYin(String inputString) {
	    StringBuilder pinyin = new StringBuilder();
	    char[] nameChar = inputString.toCharArray();
	    HanyuPinyinOutputFormat defaultFormat = new HanyuPinyinOutputFormat();
	    defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);
	    defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
	    for (int i = 0; i < nameChar.length; i++) {
	        if (nameChar[i] > 128) {
	            try {
	                String[] py = PinyinHelper.toHanyuPinyinStringArray(nameChar[i],
	                                                                    defaultFormat);
	                if (py != null) {
	                    pinyin.append(py[0].substring(0, 1) + py[0].substring(1));
	                }
	            }
	            catch (BadHanyuPinyinOutputFormatCombination e) {
	                e.printStackTrace();
	            }
	        }
	        else {
	            pinyin.append(nameChar[i]);
	        }
	    }
	    return pinyin.toString();
	}*/
	
	public static  void main(String[]  arges){
		
	}
}
