package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.apache.tomcat.jni.Local;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    public CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        //解决缓存穿透问题
//        Shop shop = cnm(id);
//        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.SECONDS);
        //利用互斥锁解决击穿问题
//        Shop shop = cacheClient.queryWithMutex(CACHE_SHOP_KEY, id, Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.SECONDS);
        //利用逻辑时间锁解决击穿问题
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.SECONDS);
        if(shop == null) return Result.fail("店铺id不存在！");
        //返回
        return Result.ok(shop);
    }
    //利用逻辑锁解决穿透问题
//    private Shop queryWithLogicalExpire(Long id){
//        String key = CACHE_SHOP_KEY + id;
//        String json = stringRedisTemplate.opsForValue().get(key);
//        //未命中
//        if(StrUtil.isBlank(json)){
//            return null;
//        }
//        //命中
//        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        JSONObject data = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//        //未过期直接返回商铺信息
//        if(expireTime.isAfter(LocalDateTime.now())){
//            return shop;
//        }
//        // 5.2.已过期，需要缓存重建
//        // 6.缓存重建
//        // 6.1.获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = lock(lockKey);
//        //获取到锁
//        if(isLock){
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try{
//                    this.saveShop2Redis(id,20L);
//                }catch (Exception e){
//                    throw new RuntimeException(e);
//                }finally {
//                    unlock(lockKey);
//                }
//            });
//        }
//        return shop;
//    }
    //重建缓存
    public void saveShop2Redis(Long id,Long seconds){
        //查询店铺数据
        Shop byId = getById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(byId);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(seconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }
//    //利用互斥锁
//    private Shop queryWithMutex(Long id){
//        String key = CACHE_SHOP_KEY+id;
//        //从redis查询缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断存在
//        if(StrUtil.isNotBlank(shopJson)){
//            //存在直接返回
//            return  JSONUtil.toBean(shopJson,Shop.class);
//        }
//        //空值判断
//        if(shopJson != null){
//            return null;
//        }
//        //未命中，考虑获得锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        //没有获得锁
//        try{
//            //尝试上锁
//            boolean isLock = lock(lockKey);
//            if(!isLock){
//                Thread.sleep(50);
//                //递归调自己
//                return queryWithMutex(id);
//            }
//            //查询数据库数据
//            shop = getById(id);
//            //模拟重建缓冲的延时
//            Thread.sleep(200);
//            //数据库中不存在，返回错误
//            if(shop == null) {
//                //在reids中设置为2min的空串
//                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//            //存在，写会redis和返回数据
//            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }finally {
//            unlock(lockKey);
//        }
//        return shop;
//    }
//    //上锁
//    private boolean lock(String  lockKey){
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//    private void unlock(String lockKey){
//        stringRedisTemplate.delete(lockKey);
//    }
//    //防穿透
////    private Shop queryWithPassThrough(Long id){
////        String key = CACHE_SHOP_KEY+id;
////        //从redis查询缓存
////        String shopJson = stringRedisTemplate.opsForValue().get(key);
////        //判断存在
////        if(StrUtil.isNotBlank(shopJson)){
////            //存在直接返回
////            return  JSONUtil.toBean(shopJson,Shop.class);
////        }
////        if(shopJson != null){
////            return null;
////        }
////        //不存在查询数据库
////        Shop shop = getById(id);
////        //数据库中不存在，返回错误
////        if(shop == null) {
////            //在reids中设置为2min的空串
////            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
////            return null;
////        }
////        //存在，写会redis和返回数据
////        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
////        return shop;
////    }

    @Override
    public Result update(Shop shop) {
        //先更新数据库，
        Long id = shop.getId();
        if(id == null) return Result.fail("店铺id不能为null！");
        updateById(shop);
        //再删除redis
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }
}
