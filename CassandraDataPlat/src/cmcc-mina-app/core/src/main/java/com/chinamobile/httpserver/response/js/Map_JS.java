package com.chinamobile.httpserver.response.js;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

import com.chinamobile.httpserver.response.CmriHttpResponse;

public class Map_JS implements CmriHttpResponse{

	private static Map_JS instance = null;
	
	public static Map_JS getInstance() {
		if(instance == null){
			return new Map_JS();
		}
        return instance;
    }
	
	public Map_JS() {}
	
	@Override
	public HttpResponseMessage execute(Object message) throws IOException {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		
		try {
            String line = null;
//            BufferedReader reader = new BufferedReader(new FileReader("res/js/map.js"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("res/js/map.js"),"UTF-8"));
            while ((line = reader.readLine()) != null) {
            	response.appendBody(line+"\n");
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
		
		
		/*
		 * 
		 * 
		response.appendBody("var map = new BMap.Map(\"container\");\n");
		
		response.appendBody("var point_x = 126.404;\n");
		response.appendBody("var point_y = 49.915;\n");
		response.appendBody("net_test();\n");
		response.appendBody("var point = new BMap.Point(point_x,point_y);\n");
		response.appendBody("var marker = new BMap.Marker(point);\n");
		
		response.appendBody("var opts = {\n");
		response.appendBody("width : 250,     // 信息窗口宽度\n");
		response.appendBody("height: 100,     // 信息窗口高度\n");
		response.appendBody("title : \"Hello\"  // 信息窗口标题\n");
		response.appendBody("}\n");
		
		response.appendBody("map.centerAndZoom(point, 15);\n");
		response.appendBody("map.addOverlay(marker);\n");
		response.appendBody("var infoWindow = new BMap.InfoWindow(\"World\", opts);  // 创建信息窗口对象\n");
		
		response.appendBody("marker.addEventListener(\"click\", function(){ \n");
		response.appendBody("this.openInfoWindow(infoWindow); \n");
		response.appendBody("});\n");
		
		response.appendBody("function net_test() {\n");
		response.appendBody("var net=false;\n");
		response.appendBody("$.ajax({\n");
		response.appendBody("type : \"POST\",\n");
		response.appendBody("url : \"../testmessage.json\",\n");
		response.appendBody("dataType : \"json\",\n");
		response.appendBody("timeout : 5000,\n");
		response.appendBody("async:false,\n");
		response.appendBody("error : function(XMLHttpRequest, textStatus, errorThrown) {\n");
		response.appendBody("alert(\"cuowu\");\n");	
		response.appendBody("net=false;\n");
		response.appendBody("},\n");
		
		response.appendBody("success : function(data) {\n");
		response.appendBody("point_x=data.lng\n");
		response.appendBody("point_y=data.lat\n");
		response.appendBody("alert(data.lng);\n");
		response.appendBody("net=true;\n");
		response.appendBody("}\n");
		response.appendBody("});\n");
		response.appendBody("return net;\n");
		response.appendBody("};\n");
		
		response.appendBody("setTimeout('net_test()',14000); //指定1秒刷新一次");
		
		 */
		
		return response;
	}

}
