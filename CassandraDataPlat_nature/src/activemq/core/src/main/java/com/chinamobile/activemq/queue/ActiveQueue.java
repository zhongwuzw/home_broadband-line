package com.chinamobile.activemq.queue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.Timer;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQPrefetchPolicy;

import com.chinamobile.activemq.consumer.ActivemqConsumer;
import com.chinamobile.activemq.producer.ActivemqProducer;

public class ActiveQueue extends Observable {
	private String queueName = "";
	private Timer timer = new Timer();
	ActiveQueue aq = null;
	private MyTask mytask = new MyTask(this);

	private Connection consumerConnection;
	private Connection producerConnection;
	private Session consumerSession;
	private Session producerSession;
	private MessageConsumer consumer;
	private MessageProducer producer;
	private MessageListener messageListener;

	// 是否持久化消息
	private boolean persistent = true;
	// 队列预取策略
	private int queuePrefetch = DEFAULT_QUEUE_PREFETCH;
	public final static int DEFAULT_QUEUE_PREFETCH = 10;

	public ActiveQueue(String queueName) {
		this.queueName = queueName;
		// 会话采用非事务级别，消息到达机制使用自动通知机制
		try {
			/*
			 * createSession(boolean transacted,int acknowledgeMode) transacted
			 * - indicates whether the session is transacted acknowledgeMode -
			 * indicates whether the consumer or the client will acknowledge any
			 * messages it receives; ignored if the session is transacted. Legal
			 * values are Session.AUTO_ACKNOWLEDGE, Session.CLIENT_ACKNOWLEDGE,
			 * and Session.DUPS_OK_ACKNOWLEDGE.
			 */
			// false 参数表示 为非事务型消息，后面的参数表示消息的确认类型
			producerConnection = ActivemqProducer.getInstance()
					.getConnectionFactory().createConnection();
			producerSession = producerConnection.createSession(Boolean.FALSE,
					Session.AUTO_ACKNOWLEDGE);
			// Destination is superinterface of Queue
			// PTP消息方式
			Destination producerDestination = producerSession
					.createQueue(this.queueName);
			// Creates a MessageProducer to send messages to the specified
			// destination
			producer = producerSession.createProducer(producerDestination);
			// set delevery mode
			producer.setDeliveryMode(this.persistent ? DeliveryMode.PERSISTENT
					: DeliveryMode.NON_PERSISTENT);

			consumerConnection = ActivemqConsumer.getInstance()
					.getConnectionFactory().createConnection();
			consumerSession = consumerConnection.createSession(Boolean.FALSE,
					Session.AUTO_ACKNOWLEDGE);
			Destination consumerDestination = consumerSession
					.createQueue(this.queueName);
			consumer = consumerSession.createConsumer(consumerDestination);
			// activeMQ预取策略
			ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
			prefetchPolicy.setQueuePrefetch(queuePrefetch);
			((ActiveMQConnection) consumerConnection)
					.setPrefetchPolicy(prefetchPolicy);
			consumerConnection.start();

			aq = this;
			this.addObserver(QueueList.getInstance());
			this.timer.schedule(mytask, 3000, 2000);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public Timer getTimer() {
		return timer;
	}

	public void setTimer(Timer timer) {
		this.timer = timer;
	}

	public void used() {
		this.timer.purge();
		mytask.setState(0);
		this.timer.schedule(mytask, 3000, 2000);
	}

	public void delete() {
		this.setChanged();
		this.notifyObservers(this);
	}

	public void shutdown() {
		if (consumerSession != null) {
			try {
				consumerSession.close();
				consumerConnection.close();
				producerSession.close();
				producerConnection.close();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			consumerSession = null;
			consumerConnection = null;
			producerSession = null;
			producerConnection = null;
		}
	}

	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
		try {
			consumer.setMessageListener(this.messageListener);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 执行发送消息的具体方法
	 * 
	 * @param queue
	 * @param map
	 */
	public void send(final Map<String, Object> map) {
		// 直接使用线程池来执行具体的调用
		ActivemqProducer.getInstance().getThreadPool().execute(new Runnable() {
			@Override
			public void run() {
				try {
					Message message = getMessage(producerSession, map);
					producer.send(message);
					aq.used();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * 执行发送消息的具体方法
	 * 
	 * @param queue
	 * @param map
	 */
	public Message recive() {
		try {
			aq.used();
			return consumer.receive(3000);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private Message getMessage(Session session, Map<String, Object> map)
			throws JMSException {
		MapMessage message = session.createMapMessage();
		if (map != null && !map.isEmpty()) {
			Set<String> keys = map.keySet();
			for (String key : keys) {
				message.setObject(key, map.get(key));
			}
		}
		return message;
	}
}
