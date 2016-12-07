package redis;

import java.io.File;
import java.util.ResourceBundle;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolManage {
	private static JedisPool pool = null;
	private static String onoff;
	private static String password;
	private static String maxactive;
	private static String maxidle;
	private static String MaxWait;
	private static String ip;
	private static String port;
	/**
    * 构建redis连接池
    * 
    * @param ip
    * @param port
    * @return JedisPool
	 * @throws Exception 
    */
   public static JedisPool getPool() throws Exception {
	   ResourceBundle LogfileInfo =  ResourceBundle.getBundle("config"+File.separator+"redis");
	   onoff = LogfileInfo.getString("redis.onoff");
	   if("off".equals(onoff)){
		   return null;
	   }
	   password =LogfileInfo.getString("redis.password");
	   maxactive=LogfileInfo.getString("redis.maxactive");
	   maxidle=LogfileInfo.getString("redis.maxidle");
	   MaxWait=LogfileInfo.getString("redis.maxwait");
	   ip=LogfileInfo.getString("redis.ip");
	   port=LogfileInfo.getString("redis.port");
       if (pool == null) {
           JedisPoolConfig config = new JedisPoolConfig();
           //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
           //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
           config.setMaxActive(Integer.valueOf(maxactive));
           
           //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
           config.setMaxIdle(Integer.valueOf(maxidle));
           //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
           config.setMaxWait(1000 * Integer.valueOf(MaxWait));
           //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
           config.setTestOnBorrow(true);
           if(password!=null && !"".equals(password)){
        	   pool = new JedisPool(config, ip, Integer.valueOf(port),Integer.valueOf(MaxWait),password);
           }else{
        	   pool = new JedisPool(config, ip, Integer.valueOf(port));
           }
       }
       return pool;
   }
   
   /**
    * 返还到连接池
    * 
    * @param pool 
    * @param redis
    */
   public static void returnResource(JedisPool pool, Jedis redis) {
       if (redis != null) {
           pool.returnResource(redis);
       }
   }
   
   /**
    * 获取数据
    * 
    * @param key
    * @return
    */
   public static String get(String key){
       String value = null;
       
       JedisPool pool = null;
       Jedis jedis = null;
       try {
           pool = getPool();
           jedis = pool.getResource();
           value = jedis.get(key);
       } catch (Exception e) {
           //释放redis对象
           pool.returnBrokenResource(jedis);
           e.printStackTrace();
       } finally {
           //返还到连接池
           returnResource(pool, jedis);
       }
       
       return value;
   }
}

