package com.chinamobile.activemq.consumer;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * JMS消息消费者
 * 
 * @author wjwd
 * 
 */
public class ActivemqConsumer implements ExceptionListener {

	private String brokerUrl;

	private String userName;

	private String password;

	private ConnectionFactory connectionFactory;

	static private ActivemqConsumer instance = null;

	public static synchronized ActivemqConsumer getInstance() {
		if (instance == null) {
			instance = new ActivemqConsumer();
		}
		return instance;
	}

	/**
	 * 执行消息获取的操作
	 * 
	 * @throws Exception
	 */
	public void init() {
		// ActiveMQ的连接工厂
		connectionFactory = new ActiveMQConnectionFactory(this.userName,
				this.password, this.brokerUrl);
	}

	@Override
	public void onException(JMSException e) {
		e.printStackTrace();
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

	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

}
