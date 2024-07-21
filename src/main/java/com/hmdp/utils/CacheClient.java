package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

/**
 * @Author: likelihood
 * @Description:
 * @Date 2024/7/20 1:04
 */
@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    //开启线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //封装逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    //防穿透
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id,
                                          Class<R> type, Function<ID,R> dbFallback,
                                          Long time, TimeUnit unit){
        String key = keyPrefix+id;
        //从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断存在
        if(StrUtil.isNotBlank(json)){
            //存在直接返回
            return  JSONUtil.toBean(json,type);
        }
        if(json != null){
            return null;
        }
        //不存在查询数据库
        R r = dbFallback.apply(id);
        //数据库中不存在，返回错误
        if(r == null) {
            //在reids中设置为2min的空串
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //存在，写回redis和返回数据
        this.set(key,r,time,unit);
        return r;
    }

    //利用互斥锁
    public  <R,ID> R queryWithMutex(String keyPrefix, ID id,Class<R> type,Function<ID,R> dbFallback,Long time, TimeUnit unit){
        String key = keyPrefix+id;
        //从redis查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断存在
        if(StrUtil.isNotBlank(json)){
            //存在直接返回
            return  JSONUtil.toBean(json,type);
        }
        //空值判断
        if(json != null){
            return null;
        }
        //未命中，考虑获得锁
        String lockKey = LOCK_SHOP_KEY + id;
        R r = null;
        //没有获得锁
        try{
            //尝试上锁
            boolean isLock = lock(lockKey);
            if(!isLock){
                Thread.sleep(50);
                //递归调自己
                return queryWithMutex(keyPrefix,id,type,dbFallback,time,unit);
            }
            //查询数据库数据
            r = dbFallback.apply(id);
            //模拟重建缓冲的延时
            Thread.sleep(200);
            //数据库中不存在，返回错误
            if(r == null) {
                //在reids中设置为2min的空串
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //存在，写会redis和返回数据
            this.set(key,r,time,unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(lockKey);
        }
        return r;
    }
    //利用逻辑锁解决穿透问题
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id,Class<R> type,
                                            Function<ID,R> dbFallback,
                                            Long time, TimeUnit unit){
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //未命中
        if(StrUtil.isBlank(json)){
            return null;
        }
        //命中
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        //未过期直接返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        // 5.2.已过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = lock(lockKey);
        //获取到锁
        if(isLock){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try{
                    //重建缓存
                    R apply = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key,apply,time,unit);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    unlock(lockKey);
                }
            });
        }
        return r;
    }
    private boolean lock(String  lockKey){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String lockKey){
        stringRedisTemplate.delete(lockKey);
    }


}
