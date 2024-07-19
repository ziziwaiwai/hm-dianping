package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

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
    @Override
    public Result queryById(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY+id;
        //从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在直接返回
            return  Result.ok(JSONUtil.toBean(shopJson,Shop.class));
        }
        //不存在查询数据库
        Shop shop = getById(id);
        //不存在，返回错误
        if(shop == null) return Result.fail("店铺不存在！");
        //存在，写会redis和返回数据
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop));
        //返回
        return Result.ok(shop);
    }
}
