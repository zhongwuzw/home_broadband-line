package com.cmri.common.redis;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class LicenseJedisCilent {
	private static Logger logger = Logger.getLogger(LicenseJedisCilent.class);

	private static JedisPool licenseJedisPool;

	public static JedisPool getLicenseJedisPool() {
		return licenseJedisPool;
	}

	public static void setLicenseJedisPool(JedisPool licenseJedisPool) {
		LicenseJedisCilent.licenseJedisPool = licenseJedisPool;
	}

	public static String insertObj(String sessionid, int timeout, Serializable obj) {
		logger.info("redis-key:" + sessionid + "----timeout:" + timeout);
		byte[] b = serialize(obj);
		if(b == null){
			return null;
		}
		Jedis jedis = null;
		try{
			jedis = licenseJedisPool.getResource();
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
			jedis = licenseJedisPool.getResource();
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
			jedis = licenseJedisPool.getResource();
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
			jedis = licenseJedisPool.getResource();
			jedis.del(key);
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}

	public static String get(String key) {
		logger.info("redis-key:" + key);
		Jedis jedis = null;
		try{
			jedis = licenseJedisPool.getResource();
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
			jedis = licenseJedisPool.getResource();
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
			jedis = licenseJedisPool.getResource();
			Long setex = jedis.del(sessionid.getBytes());
			return setex;
		}finally{
			if(jedis != null){
				jedis.close();
			}
		}
	}
	
	public static void set(byte[] key, byte[] value) {
		logger.info("redis-key:" + key);
		Jedis jedis = null;
		try{
			jedis = licenseJedisPool.getResource();
			jedis.set(key, value);
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
			jedis = licenseJedisPool.getResource();
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
			jedis = licenseJedisPool.getResource();
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
			// 反序列化
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
