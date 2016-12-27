package com.cmri.common.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;





import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisCilent {

	static Logger logger = LoggerFactory.getLogger(JedisCilent.class);
	private static JedisPool jedisPool;
	
	public static JedisPool getJedisPool() {
		return jedisPool;
	}

	public static void setJedisPool(JedisPool jedisPool) {
		JedisCilent.jedisPool = jedisPool;
	}
	
	public static String insertObj(String sessionid, int timeout, Serializable obj) {
		logger.info("redis-key:" + sessionid + "----timeout:" + timeout);
		byte[] b = serialize(obj);
		if(b == null){
			return null;
		}
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			String setex = jedis.setex(sessionid.getBytes(), timeout, b);
			return setex;
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static String setex(String key, int timeout, String value) {
		logger.info("redis-key:" + key + "----timeout:" + timeout);
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			String setex = jedis.setex(key, timeout, value);
			return setex;
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}
	
	public static void rename(String key, String newKey) {
		logger.info("redis-key:" + key + "----newKey:" + newKey);
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			jedis.rename(key, newKey);
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static void del(String key) {
		logger.info("redis-key:" + key);
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			jedis.del(key);
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static String get(String key) {
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			String value = jedis.get(key);
			return value;
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static Object getObj(String sessionid) {
		logger.info("redis-key:" + sessionid);
		if (sessionid == null || "".equals(sessionid)) {
			return null;
		}
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			byte[] setex = jedis.get(sessionid.getBytes());
			Object obj = unserialize(setex);
			return obj;
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static Long delObj(String sessionid) {
		logger.info("redis-key:" + sessionid);
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			Long setex = jedis.del(sessionid.getBytes());
			return setex;
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}
	
	public static void set(String key, String value) {
		logger.info("redis-key:" + key);
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			jedis.set(key, value);
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static void expire(String key, int timeout) {
		logger.info("redis-key:" + key + "----timeout:" + timeout);
		Jedis jedis = null;
		try{
			jedis = jedisPool.getResource();
			jedis.expire(key, timeout);
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static Object unserialize(byte[] bytes) {
		if(bytes == null){
			return null;
		}
		ByteArrayInputStream bais = null;
		ObjectInputStream ois = null;
		try {
			bais = new ByteArrayInputStream(bytes);
			ois = new ObjectInputStream(bais);
			return ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bais != null) {
				try {
					bais.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (ois != null) {
				try {
					ois.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public static byte[] serialize(Object object) {
		if(object == null){
			return null;
		}
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;
		try {
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			return baos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (oos != null) {
				try {
					oos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

}
