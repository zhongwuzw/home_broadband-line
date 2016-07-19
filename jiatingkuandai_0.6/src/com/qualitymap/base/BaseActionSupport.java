package com.qualitymap.base;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.struts2.interceptor.RequestAware;
import org.apache.struts2.interceptor.ServletRequestAware;
import org.apache.struts2.interceptor.ServletResponseAware;
import org.apache.struts2.interceptor.SessionAware;

import com.opensymphony.xwork2.ActionSupport;

/**
 * 
 * @author：kxc
 * @date：Mar 29, 2016
 */
public abstract class BaseActionSupport extends ActionSupport implements SessionAware, RequestAware, ServletRequestAware, ServletResponseAware {

	/**
	 * serialVersionUID long BaseActionSupport.java
	 */
	private static final long serialVersionUID = 3449571329381182975L;

	protected HttpServletRequest servletRequest;

	protected HttpServletResponse servletResponse;

	protected Map<String, Object> session;

	private Map request;

	public void setRequest(Map arg0) {
		this.request = arg0;

	}

	public Map getRequest() {
		return this.request;

	}

	public void setServletRequest(HttpServletRequest arg0) {
		this.servletRequest = arg0;

	}

	public void setServletResponse(HttpServletResponse arg0) {
		this.servletResponse = arg0;
	}

	public void responseWrite(String string) throws IOException {
		servletResponse.setCharacterEncoding("UTF-8");
		servletResponse.getWriter().print(string);
	}

	public Map<String, Object> getSession() {
		return session;
	}

	@SuppressWarnings("unchecked")
	public void setSession(Map session) {
		this.session = session;
	}

	public HttpServletRequest getServletRequest() {
		return servletRequest;
	}

	public HttpServletResponse getServletResponse() {
		return servletResponse;
	}

}
