package com.chinamobile.activemq.producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;

/**
 * JMS消息生产者
 * 
 * @author wjwd
 * 
 */
public class ActivemqProducer implements ExceptionListener {

	public final static int DEFAULT_MAX_CONNECTIONS = 5;
	public final static int DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION = 300;
	public final static int DEFAULT_THREAD_POOL_SIZE = 50;
	private static ActivemqProducer instance = ActivemqProducer.getInstance();
	// 设置连接的最大连接数
	private int maxConnections;
	// 设置每个连接中使用的最大活动会话数
	private int maximumActiveSessionPerConnection;
	// 线程池数量
	private int threadPoolSize;
	// 强制使用同步返回数据的格式
	private boolean useAsyncSendForJMS = true;

	// 连接地址
	private String brokerUrl;
	private String userName;
	private String password;

	private ExecutorService threadPool;
	private PooledConnectionFactory connectionFactory;

	public static synchronized ActivemqProducer getInstance() {
		if (instance == null) {
			instance = new ActivemqProducer();
		}
		return instance;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
	}

	public int getMaximumActiveSessionPerConnection() {
		return maximumActiveSessionPerConnection;
	}

	public void setMaximumActiveSessionPerConnection(
			int maximumActiveSessionPerConnection) {
		this.maximumActiveSessionPerConnection = maximumActiveSessionPerConnection;
	}

	public int getThreadPoolSize() {
		return threadPoolSize;
	}

	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}

	public boolean isUseAsyncSendForJMS() {
		return useAsyncSendForJMS;
	}

	public void setUseAsyncSendForJMS(boolean useAsyncSendForJMS) {
		this.useAsyncSendForJMS = useAsyncSendForJMS;
	}

	public String getBrokerUrl() {
		return brokerUrl;
	}

	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public ExecutorService getThreadPool() {
		return threadPool;
	}

	public void setThreadPool(ExecutorService threadPool) {
		this.threadPool = threadPool;
	}

	public PooledConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setConnectionFactory(PooledConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public static int getDefaultMaxConnections() {
		return DEFAULT_MAX_CONNECTIONS;
	}

	public static int getDefaultMaximumActiveSessionPerConnection() {
		return DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION;
	}

	public static int getDefaultThreadPoolSize() {
		return DEFAULT_THREAD_POOL_SIZE;
	}

	public void init() {
		if (this.maxConnections <= 0) {
			this.maxConnections = DEFAULT_MAX_CONNECTIONS;
		}
		if (this.maximumActiveSessionPerConnection <= 0) {
			this.maximumActiveSessionPerConnection = DEFAULT_MAXIMUM_ACTIVE_SESSION_PER_CONNECTION;
		}
		if (this.threadPoolSize <= 0) {
			this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
		}

		// 设置JAVA线程池
		this.threadPool = Executors.newFixedThreadPool(this.threadPoolSize);
		// ActiveMQ的连接工厂
		ActiveMQConnectionFactory actualConnectionFactory = new ActiveMQConnectionFactory(
				this.userName, this.password, this.brokerUrl);
		actualConnectionFactory.setUseAsyncSend(this.useAsyncSendForJMS);
		// Active中的连接池工厂
		this.connectionFactory = new PooledConnectionFactory(
				actualConnectionFactory);
		this.connectionFactory.setCreateConnectionOnStartup(true);
		this.connectionFactory.setMaxConnections(this.maxConnections);
		this.connectionFactory
				.setMaximumActiveSessionPerConnection(this.maximumActiveSessionPerConnection);
	}

	@Override
	public void onException(JMSException e) {
		e.printStackTrace();
	}

}
