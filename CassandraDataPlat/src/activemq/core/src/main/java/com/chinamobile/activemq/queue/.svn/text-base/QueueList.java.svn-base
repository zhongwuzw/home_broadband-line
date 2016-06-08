package com.chinamobile.activemq.queue;

import java.util.HashMap;
import java.util.Observable;
import java.util.Observer;

public class QueueList implements Observer {

	static private QueueList instance = null;
	private HashMap<String, ActiveQueue> queueController = new HashMap<String, ActiveQueue>();

	public static synchronized QueueList getInstance() {
        if (instance == null){
        	instance = new QueueList();
        }
        return instance;
    }
	
	public HashMap<String, ActiveQueue> getQueueController() {
		return queueController;
	}

	public void setQueueController(HashMap<String, ActiveQueue> queueController) {
		this.queueController = queueController;
	}
	
	public QueueList(Observable observable) {
		observable.addObserver(this); // 注册关系
	}

	public QueueList() {
		// TODO Auto-generated constructor stub
	}

	public void add(ActiveQueue aq) {
		// TODO Auto-generated method stub
		if (aq == null || queueController.containsKey(aq.getQueueName())) {
			return;
		}
		queueController.put(aq.getQueueName(), aq);
	}

	public void delete(ActiveQueue aq) {
		// TODO Auto-generated method stub
		if (aq != null && queueController.containsKey(aq.getQueueName())) {
			queueController.remove(aq.getQueueName());
		}
	}

	public ActiveQueue get(String queueName) {
		// TODO Auto-generated method stub
		if (queueName != null) {
			if (queueController.containsKey(queueName)) {
				return queueController.get(queueName);
			} else {
				ActiveQueue aq = new ActiveQueue(queueName);
				this.add(aq);
				return aq;
			}
		}
		return null;
	}

	public boolean contains(String queueName) {
		// TODO Auto-generated method stub
		if (queueName != null) {
			if (!queueController.containsKey(queueName))
				return true;
		}
		return false;
	}
	
	@Override
	public void update(Observable observable, Object arg1) {
		// TODO Auto-generated method stub
		if (observable instanceof ActiveQueue) {
			((ActiveQueue) arg1).shutdown();
			this.delete((ActiveQueue) arg1);
		}
	}

}
