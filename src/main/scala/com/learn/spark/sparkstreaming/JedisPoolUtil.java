package com.learn.spark.sparkstreaming;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 双重检查锁实现懒汉式单例
 * 采用懒汉式的原因是spark是惰性求值，没有数据时不应该触发jedis的操作，为考虑性能采用懒汉式设计模式
 */
public class JedisPoolUtil {

    private static volatile JedisPool jedisPool = null;
    private static final String hostname = "localhost";
    private static final int port = 6379;

    public static Jedis getConnection(){
        if(jedisPool == null){
            synchronized (JedisPoolUtil.class){
                if(jedisPool == null){
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(30);
                    config.setMaxIdle(10);
                    jedisPool = new JedisPool(config, hostname, port);
                }
            }
        }
        return jedisPool.getResource();
    }

}
