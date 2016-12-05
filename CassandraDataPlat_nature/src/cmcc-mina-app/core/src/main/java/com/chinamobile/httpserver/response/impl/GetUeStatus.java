package com.chinamobile.httpserver.response.impl;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Properties;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;
import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.data.DataParser;
import com.chinamobile.httpserver.response.CmriHttpResponse;

public class GetUeStatus implements CmriHttpResponse{

	private static GetUeStatus instance = null;
	
	public static GetUeStatus getInstance() {
		if(instance == null){
			return new GetUeStatus();
		}
        return instance;
    }
	
	public GetUeStatus() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {	
		String callbackparam = "";
		String value = ""; 
		
		callbackparam = ((HttpRequestMessage)message).getParameter("callbackparam");
		value = ((HttpRequestMessage)message).getParameter("value");
		System.out.println("callbackparam : "+callbackparam);
		System.out.println("value : "+value);
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.setContentType("application/json; charset=utf-8");
		InputStream fis = null; 
		BufferedReader br = null;
		try{
			String url = value;
			System.out.println("url��ַΪ��"+url);
			
			HttpClient client = new DefaultHttpClient();//����һ�����������http�ͻ���
			
			HttpGet httpget = new HttpGet();
			httpget.setURI(new URI(url));
			HttpResponse responseFir = client.execute(httpget); 

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
		
		
		System.out.println("������������Ϣ��"+value);
		
		return response;
	}

}

