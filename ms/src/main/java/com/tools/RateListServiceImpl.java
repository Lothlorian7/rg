package cn.bitoffer.msgcenter.tools;

import cn.bitoffer.msgcenter.conf.SendMsgConf;
import cn.bitoffer.msgcenter.constant.Constants;
import cn.bitoffer.msgcenter.mapper.GlobalQuotaMapper;
import cn.bitoffer.msgcenter.mapper.SourceQuotaMapper;
import cn.bitoffer.msgcenter.model.GlobalQuotaModel;
import cn.bitoffer.msgcenter.model.SourceQuotaModel;
import cn.bitoffer.msgcenter.model.TemplateModel;
import cn.bitoffer.msgcenter.tools.RateLimitService;
import cn.bitoffer.msgcenter.utils.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class RateListServiceImpl implements RateLimitService {

    @Autowired
    private GlobalQuotaMapper globalQuotaMapper;

    @Autowired
    private SourceQuotaMapper sourceQuotaMapper;

    @Resource
    RedisTemplate<String, String> redisTemplate;

    @Autowired
    SendMsgConf sendMsgConf;

    @Override
    public boolean isRequestAllowed(String sourceId,int channel) {
        int numLimit = 0;
        int unit = 0;

        //先从渠道限额获取配置
        String quotaConf = getQuotaConfWithCache(channel,sourceId);
        if(StringUtils.isEmpty(quotaConf) || (quotaConf.split("_").length != 2)){
            return false;
        }
        String[] parts = quotaConf.split("_");
        numLimit = Integer.parseInt(parts[0]);
        unit = Integer.parseInt(parts[1]);


        // 根据配置，调用具体限额判断逻辑
        return checkAllowed(sourceId,numLimit,unit);
    }

    private String getQuotaConfWithCache(int channel,String sourceId) {
        String quotaCacheKey = Constants.REDIS_KEY_SOURCE_QUOTA+sourceId;
        String cacheQt = redisTemplate.opsForValue().get(quotaCacheKey);

        String rsQuotaStr = "";
        // 1. 开启缓存且有值，直接返回
        if(!StringUtils.isEmpty(cacheQt) && sendMsgConf.isOpenCache()){
            rsQuotaStr = cacheQt;
            return rsQuotaStr;
        }else{
            //2.从数据库获取配置

            //先从渠道限额获取配置
            SourceQuotaModel sourceQuota = sourceQuotaMapper.getSourceQuota(channel,sourceId);
            if(sourceQuota != null){
                rsQuotaStr = sourceQuota.getNum()+"_"+sourceQuota.getUnit();
            }else {
                // 如果没有配置渠道限额，那就从全局限额获取配置
                GlobalQuotaModel globalQuota = globalQuotaMapper.getGlobalQuota(channel);
                if (globalQuota != null){
                    rsQuotaStr = globalQuota.getNum()+"_"+globalQuota.getUnit();
                }
            }
        }

        // 缓存起来
        if(sendMsgConf.isOpenCache() && !StringUtils.isEmpty(rsQuotaStr)){
            redisTemplate.opsForValue().set(quotaCacheKey,rsQuotaStr,Duration.ofSeconds(30));
        }

        return rsQuotaStr;
    }

    public boolean checkAllowed(String sourceID, int limit, long div) {
        long currentStart = System.currentTimeMillis() / div;
        String key = String.format("rate_limit:%s:%d", sourceID, currentStart);

        // 递增计数器
        Long count = redisTemplate.opsForValue().increment(key, 1);
        if (count == null){
            log.error("增加计数失败");
            return  false;
        }
        // 如果是第一次累加，则设置一个时间一秒限时
        if (count == 1) {
            long expireSeconds = div / 1000;
            redisTemplate.expire(key, expireSeconds, TimeUnit.SECONDS);
        }

        // 比较检查是否超过限制次数limit
        return count <= limit;
    }
}
