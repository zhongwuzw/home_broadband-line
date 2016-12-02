package com.chinamobile.httpserver.data;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;

public class XMLEdit {

	public XMLEdit(){}
	
	public void readXMLContent() {
		XMLOutputter out = new XMLOutputter();
	    SAXBuilder builder = new SAXBuilder();
	    try {
	        Document doc = builder.build(new File("res/messagemodule.xml"));
	        Element rootEl = doc.getRootElement();
	        //获得所有子元素
	        List<Element> list = rootEl.getChildren();
	        for (Element el : list) {
	            //获取子元素文本值
	            String uid = el.getChildText("uid");
	            String lng = el.getChildText("lng");
	            String lat = el.getChildText("lat");
	            String content = el.getChildText("content");
	            System.out.println("测试ID:" + uid);
	            System.out.println("经度:" + lng);
	            System.out.println("纬度:" + lat);
	            System.out.println("消息内容:" + content);
	            System.out.println("-----------------------------------");
	            System.out.println(out.outputString(doc));
	        }
	    } catch (JDOMException e) {
	        e.printStackTrace();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	
	public String creatXMLContent(TestMessageBean tmb) {
		XMLOutputter out = new XMLOutputter();
	    SAXBuilder builder = new SAXBuilder();
	    String xmlContents = "";
	    try {
	        Document doc = builder.build(new File("res/messagemodule.xml"));
	        Element rootEl = doc.getRootElement();
	        //获得所有子元素
	        List<Element> list = rootEl.getChildren();
	        for (Element el : list) {
	            //获取子元素文本值
	            el.getChild("uid").setText(Integer.toString(tmb.getUid()));
	            el.getChild("lng").setText(Float.toString(tmb.getLng()));
	            el.getChild("lat").setText(Float.toString(tmb.getLat()));
	            el.getChild("content").setText(tmb.getContents());
	            System.out.println("-----------------------------------");
	            xmlContents = out.outputString(doc);
	            System.out.println("源信息： "+new String(xmlContents.getBytes("ascii"),"UTF-8"));
	            xmlContents = new String(xmlContents.getBytes(),"UTF-8");
	            System.out.println("转换后信息信息： "+xmlContents);
	        }
	    } catch (JDOMException e) {
	        e.printStackTrace();
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
		return xmlContents;
	}

}
