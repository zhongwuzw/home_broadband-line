package com.chinamobile.util;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Text;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import com.chinamobile.httpserver.response.xml.PostXmlMessage;

public class UpdateApplyRespXML {
	
	static Logger logger = Logger.getLogger(UpdateApplyRespXML.class);

	static public String BuildXML(String status,String updateURL,String uploadCode,String Message) {

		// 创建根节点...
		Element root = new Element("UploadApplyResponse");
		root.setAttribute("Status", status);
		// 将根节点添加到文档中...
		Document Doc = new Document(root);
		
		//Element el_updateURL = new Element("UploadUrl");
		//el_updateURL.addContent(updateURL);
		//root.addContent(el_updateURL);
		
		Element el_uploadCode = new Element("UploadCode");
		el_uploadCode.addContent(uploadCode);
		root.addContent(el_uploadCode);
				
		Element el_message = new Element("Message");
		el_message.addContent(Message);
		root.addContent(el_message);

		XMLOutputter XMLOut = new XMLOutputter(FormatXML());
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		
		try {
			XMLOut.output(Doc,bo);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//logger.debug(bo.toString());
		
		return bo.toString();
	}

	static public Format FormatXML() {
		// 格式化生成的xml文件，如果不进行格式化的话，生成的xml文件将会是很长的一行...
		Format format = Format.getCompactFormat();
		format.setEncoding("utf-8");
		format.setIndent(" ");
		return format;
	}
}
