package com.ijunhai.common.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.ijunhai.common.redis.RedisClientConstants.*;
import static org.apache.commons.pool2.impl.BaseObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
import static org.apache.commons.pool2.impl.GenericObjectPoolConfig.DEFAULT_MIN_IDLE;


public class RedisClientAlone {
    private static JedisPool pool = null;
    private static RedisClientAlone instance = null;
    static JedisPoolConfig config = new JedisPoolConfig();
    RedisClientAlone(){
        if (pool == null) {
            String host = "redis-client";
//            String host = "192.168.1.110";
            int port = 6379;
            // set jedis instance amount, default 8
            config.setMaxTotal(PropertiesUtils.getInt(REDIS_MaxTotal, 1000));
            // set max idle jedis instance amount, default 8
            config.setMaxIdle(PropertiesUtils.getInt(REDIS_MaxIdle, 1000));
            // set min idle jedis instance amount, default 0
            config.setMinIdle(PropertiesUtils.getInt(REDIS_MinIdle, DEFAULT_MIN_IDLE));
            // max wait time when borrow a jedis instance
            config.setMaxWaitMillis(PropertiesUtils.getLong(REDIS_MaxWaitMillis, DEFAULT_MAX_WAIT_MILLIS));
            config.setTestOnBorrow(true);
            int REDIS_TIMEOUT = 3000;
            pool = new JedisPool(config, host, port, REDIS_TIMEOUT);
        }
    }

    public static RedisClientAlone getInstatnce(){
        if (instance == null) {
            synchronized (RedisClient.class) {
                if (instance == null) {
                    instance = new RedisClientAlone();
                }
            }
        }
        return instance;
    }

    public static JedisPool getPool() {
        return pool;
    }

    public Jedis getJedis(){
        Jedis jedis = null;
        JedisPool pool = getPool();
        if(pool != null){
            jedis = pool.getResource();
        }
        return jedis;
    }



    public static String get(Jedis jedis, String key){
        String value = null;
        try {
            value = jedis.get(key);
        } catch (Exception e) {
            //释放redis对象
            jedis.close();
            e.printStackTrace();
        }
        return value;
    }


    public static Long appendKey(Jedis jedis,String key,String value){
        return jedis.append(key, value);
    }

    public static void resetKey(Jedis jedis,String key,String value){
        jedis.set(key,value);
    }

    public static void deleteKey(Jedis jedis,String key){
        jedis.del(key);
    }

    public Boolean isExist(Jedis jedis,String key){
        return jedis.exists(key);
    }

    public static String setLifeExist(Jedis jedis,int seconds,String key,String value){
        return jedis.setex(key, seconds, value);
    }

    public static Long setKeyExpire(Jedis jedis,int seconds,String key){
        return jedis.expire(key, seconds);
    }

    public static String getSubVal(Jedis jedis,String key, int startOffset, int endOffset){
        return jedis.getrange(key,startOffset,endOffset);
    }

    public static List<String> getAllKeys(Jedis jedis){
        List<String> lst = new ArrayList<String>();
        Set s = jedis.keys("*");
        Iterator it = s.iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            String value = jedis.get(key);
            lst.add("key: "+key+" value: "+value);
        }
        return lst;
    }

    public static void returnResource(Jedis jedis) throws IOException {
        if(jedis!=null){
            try {
                pool.returnResource(jedis);
//                jedis.close();//3.0版本开
            }catch (Exception e){
                e.printStackTrace();
                jedis=null;
            }
            System.out.println("in close redis conn!");
        }
    }
}
