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
			// �жϷ�����������
			if (acceptor.isActive()) {
				logger.info("д��ʱ: 10000ms");
				logger.info("Idle����: Both Idle 90s");
				logger.info("�˿�����: true");
				logger.info("����˳�ʼ�����....");
				logger.info("����������....��ʼ����...." + acceptor.getLocalAddresses());
				logger.info("�߳�����" + threads);
				logger.info("start IPs loaded.." + startips.size());
				logger.info("location list loaded.." + locations.size());
				logger.info("total loading time.." + (end - start) + " ms");
				logger.info("��������ʱ.." + (end2 - start) + " ms");
			} else {
				logger.info("����˳�ʼ��ʧ��....");
			}
			// TODO Auto-generated constructor stub

		} catch (Exception e) {
			// TODO: handle exception
		}

	}
}