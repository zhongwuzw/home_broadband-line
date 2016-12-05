package com.chinamobile.activemq.consumer;

import javax.jms.Message;


/**
 * 提供消息操作的回调接口
 * @author wjwd
 *
 */
public interface MessageHandler {

	
	/**
	 * 消息回调提供的调用方法
	 * @param message
	 */
	public void handle(Message message);
}