package com.chinamobile.httpserver.response.impl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Properties;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class ResetUE implements CmriHttpResponse{

	private static ResetUE instance = null;
	
	public static ResetUE getInstance() {
		if(instance == null){
			return new ResetUE();
		}
        return instance;
    }
	
	public ResetUE() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {	
		String callbackparam = "";
		String value = ""; 
		String cmdstr = ""; 
		
		callbackparam = ((HttpRequestMessage)message).getParameter("callbackparam");
		value = ((HttpRequestMessage)message).getParameter("value");
		cmdstr = ((HttpRequestMessage)message).getParameter("cmd");
		System.out.println("callbackparam : "+callbackparam);
		System.out.println("value : "+value);
		System.out.println("cmdstr : "+cmdstr);
		cmdstr = URLDecoder.decode(cmdstr,"UTF-8");
		System.out.println("cmdstr decode: "+ cmdstr);
		
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.setContentType("application/json; charset=utf-8");
		InputStream fis = null; 
		BufferedReader br = null;
		try{
			String url = value;
			System.out.println("url地址为："+url);
			
			HttpClient client = new DefaultHttpClient();//定义一个发送请求的http客户端
			client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 60000); 
			//读取超时
			client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 60000);
			
			HttpPut httpput = new HttpPut();
			httpput.addHeader("Content-Type", "application/json");
			httpput.addHeader("Accept", "application/json");
			httpput.setURI(new URI(url));
			StringEntity se = new StringEntity(cmdstr); 
			httpput.setEntity(se);
			HttpResponse responseFir = client.execute(httpput); 

			fis = responseFir.getEntity().getContent();
			
			br = new BufferedReader(new InputStreamReader(fis,"UTF-8"));
			String line = null;
			response.appendBody(callbackparam+"(");
			while((line = br.readLine())!=null)
			{
				response.appendBody(line);
				response.appendBody("\n\r");
				System.out.println(line);
			}
			response.appendBody(")");
			fis.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		System.out.println("请求搜索的消息：");
		
		return response;
	}

}

