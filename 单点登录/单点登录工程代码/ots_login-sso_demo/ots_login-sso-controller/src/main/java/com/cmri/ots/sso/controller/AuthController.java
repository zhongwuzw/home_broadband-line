package com.cmri.ots.sso.controller;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.cmri.base.bean.SessionUser;
import com.cmri.common.redis.JedisCilent;
import com.cmri.ots.sso.AuthService;
import com.cmri.ots.sso.util.ControllerUtils;

@Controller
@RequestMapping("/action")
public class AuthController {

	@Autowired
	private AuthService authserviceImpl;
	
	@RequestMapping("/publicLogin")
	public void publicLogin(HttpServletRequest req,HttpServletResponse res) throws Exception{
		String jsonParam = req.getParameter("jsonParam");
		String username =  req.getParameter("username");
		String password =  req.getParameter("password");
		String referer = req.getParameter("referer");
		String UserAgent = req.getParameter("UserAgent");
		String RemoteAddr = req.getParameter("RemoteAddr");
		System.out.println("publicLogin:----------------jsonParam=" + jsonParam
				+ "&username=" + username + "&password=" + password);
		if (jsonParam != null) {
			JSONObject jsonObject = JSONObject.fromObject(jsonParam);
			username = jsonObject.optString("username");
			password = jsonObject.optString("password");
			referer = jsonObject.optString("referer");
			UserAgent = jsonObject.optString("UserAgent");
			RemoteAddr = jsonObject.optString("RemoteAddr");
		}
		String loginResult = authserviceImpl.publicLogin(username, password, referer, UserAgent, RemoteAddr);
		ControllerUtils.printJsonWriter(req, res, loginResult);
	}
	
	
	
	@RequestMapping("/validLogin")
	public void validLogin(HttpServletRequest req,HttpServletResponse res) throws IOException {
		String uuidkey = req.getParameter("key");
		String result = authserviceImpl.validLogin(uuidkey);
		ControllerUtils.printJsonWriter(req,res,result);
		
	}
	@RequestMapping("/getOrgInfo")
	public void getOrgInfo(HttpServletRequest req, HttpServletResponse res) throws Exception{
		String key = req.getParameter("key");
		String obj = authserviceImpl.getOrgInfo(key);
		ControllerUtils.printJsonWriter(req,res,obj);
	}
	@RequestMapping("/getUserInfo")
	public void getUserInfo(HttpServletRequest req, HttpServletResponse res) throws Exception{
		String apikey = req.getParameter("key");
		String obj = authserviceImpl.getUserInfo(apikey);
		ControllerUtils.printJsonWriter(req,res,obj);
	}
	@RequestMapping("/getOrgInfotwo")
	public void getOrgInfotwo(HttpServletRequest req, HttpServletResponse res) throws IOException{
		String key = req.getParameter("key");
		String result = authserviceImpl.queryByUserGroup(key);
		ControllerUtils.printJsonWriter(req,res,result);
	}
	@RequestMapping("/getInfo")
	public void getInfo(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String key = req.getParameter("key");
		//将个人信息对象转化成JSON
		String jsonData = authserviceImpl.getInfo(key);
		//将JSON数据写入到客户端
		ControllerUtils.printJsonWriter(req,res,jsonData);
	}
	
	/**
	 * 修改用户密码
	 * 
	 * @throws Exception
	 */
	@RequestMapping("/modifyPassword")
	public void modifyPassword(HttpServletRequest req, HttpServletResponse res) throws Exception {
		String jsonParam = req.getParameter("jsonParam");
		String result = authserviceImpl.modifyPassword(jsonParam);
		ControllerUtils.printJsonWriter(req,res,result);
	}
	@RequestMapping("/changeOrg")
	public void changeOrg(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String jsonParam = req.getParameter("jsonParam");
		String key = req.getParameter("key");
		try {
			authserviceImpl.changeOrg(jsonParam,key);
			ControllerUtils.printJsonWriter(req,res,new JSONObject().put("status", 1).toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			ControllerUtils.printJsonWriter(req,res,new JSONObject().put("status", 2).toString());
		}
	}
	@RequestMapping("/logoutAction")
	public void logoutAction(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String key = req.getParameter("key");
		JSONObject obj = new JSONObject();
		try {
			authserviceImpl.logout(key);
			obj.put("status", 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			obj.put("status", 2);
		}
		ControllerUtils.printJsonWriter(req,res,obj.toString());
	}
	
	public SessionUser returnSessionUser(HttpServletRequest req) {//--����SessionUser
		String uuid = req.getParameter("key");
		if(uuid != null && !"".equals(uuid)){
			Object obj = JedisCilent.getObj(uuid);
			if(obj !=null){
				SessionUser ruser =	(SessionUser)obj;
				return ruser;
			}
		}
		return null;
	}
	
}
