package com.chinamobile.lab.socketserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
* 简单Mina Server示例
* @author javaFound
* @www.javaKe.com
*/
public class MainServer {
	public MainServer(){
		final Logger logger = LoggerFactory.getLogger(MainServer.class); 
		logger.info("Today is {}, Temperature set to {}. Old temperature was {}."); 
		//创建一个非阻塞的Server端Socket,用NIO
		SocketAcceptor acceptor = new NioSocketAcceptor();
		//创建接收数据的过滤器
		DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
		//设定这个过滤器将一行一行(/r/n)的读取数据
		chain.addLast("myChin", new ProtocolCodecFilter(new
		TextLineCodecFactory()));
		//设定服务器端的消息处理器:一个SamplMinaServerHandler对象,
		acceptor.setHandler(new SamplMinaServerHandler());
		// 服务器端绑定的端口
		int bindPort=9988;
		//绑定端口,启动服务器
		try {
			acceptor.bind(new InetSocketAddress(bindPort));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Mina Server is Listing on:= " + bindPort);
	}
}