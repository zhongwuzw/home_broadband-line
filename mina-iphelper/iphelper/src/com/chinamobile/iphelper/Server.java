package com.chinamobile.iphelper;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class Server {
	private static final int DEFAULT_PORT = 8000;
	private static final int DEFAULT_THREADS = 5;

	public static List<Long> startips = null;
	public static List<String> locations = null;
	
	static Logger logger = Logger.getLogger(Server.class);
	
	public Server() {
		// TODO Auto-generated constructor stub
		this(new String[0]);
	}
	
	public Server(String[] args) {
		int port = DEFAULT_PORT;
		int threads = DEFAULT_THREADS;
		
//		Parameters p = new Parameters();
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-port")) {
				port = Integer.parseInt(args[i + 1]);
			} else if (args[i].equals("-thread")) {
				threads = Integer.parseInt(args[i + 1]);
			}
		}

		logger.info("start loading start IPs..");
		startips = LoadStartIPDataFromFile.loadStartIP("data/startip.txt");

		logger.info("start IPs loaded.");
		logger.info("start loading Locations.");
		locations = LoadLocDataFromFile.loadLocationJson("data/location.txt");
		logger.info("locations loaded..");
		long end = System.currentTimeMillis();

		try {
			NioSocketAcceptor acceptor = new NioSocketAcceptor(threads);
			acceptor.setBacklog(0);
			acceptor.setReuseAddress(true);
			acceptor.getSessionConfig().setWriteTimeout(10000);
			acceptor.getSessionConfig().setBothIdleTime(10);
			acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ServerProtocolCodecFactory()));
			acceptor.setHandler(new ServerHandler());
			acceptor.bind(new InetSocketAddress(port));
			long end2 = System.currentTimeMillis();
			// 判断服务端启动与否
			if (acceptor.isActive()) {
				logger.info("写超时: 10000ms");
				logger.info("Idle配置: Both Idle 90s");
				logger.info("端口重用: true");
				logger.info("服务端初始化完成....");
				logger.info("服务已启动....开始监听...." + acceptor.getLocalAddresses());
				logger.info("线程数：" + threads);
				logger.info("start IPs loaded.." + startips.size());
				logger.info("location list loaded.." + locations.size());
				logger.info("total loading time.." + (end - start) + " ms");
				logger.info("启动总用时.." + (end2 - start) + " ms");
			} else {
				logger.info("服务端初始化失败....");
			}
			// TODO Auto-generated constructor stub

		} catch (Exception e) {
			// TODO: handle exception
		}

	}
}