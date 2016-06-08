package com.qualitymap.utils;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

public class Log4j extends HttpServlet {
	private static final transient Logger log = Logger.getLogger(Log4j.class);

	public void destroy() {
		super.destroy();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		response.setContentType("text/html;charset=utf-8");
		PrintWriter out = response.getWriter();
		log.info("记录日志信息，将在控制台输出");
		log.error("可以记录错误信息，输出字体为控制");
		out.flush();
		out.close();
	}

	public void init() throws ServletException {

	}
}