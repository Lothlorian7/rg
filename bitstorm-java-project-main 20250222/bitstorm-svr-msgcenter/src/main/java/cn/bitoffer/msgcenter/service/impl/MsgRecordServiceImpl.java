package cn.bitoffer.msgcenter.service.impl;

import cn.bitoffer.msgcenter.conf.SendMsgConf;
import cn.bitoffer.msgcenter.constant.Constants;
import cn.bitoffer.msgcenter.mapper.MsgRecordMapper;
import cn.bitoffer.msgcenter.mapper.TemplateMapper;
import cn.bitoffer.msgcenter.model.MsgRecordModel;
import cn.bitoffer.msgcenter.model.TemplateModel;
import cn.bitoffer.msgcenter.service.MsgRecordService;
import cn.bitoffer.msgcenter.utils.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;

@Service
@Slf4j
public class MsgRecordServiceImpl implements MsgRecordService {

    @Autowired
    private MsgRecordMapper msgRecordMapper;

    @Autowired
    private SendMsgConf sendMsgConf;

    @Resource
    private RedisTemplate<String,String> redisTemplate;


    @Override
    public MsgRecordModel GetMsgRecordWithCache(String msgId) {
        return getMsgRecordWithCache(msgId);
    }

    public MsgRecordModel getMsgRecordWithCache(String msgId) {
        String msgRecordCacheKey = Constants.REDIS_KEY_MES_RECORD+msgId;
        String cacheMr = redisTemplate.opsForValue().get(msgRecordCacheKey);
        MsgRecordModel mr = null;
        if(!StringUtils.isEmpty(cacheMr) && sendMsgConf.isOpenCache()){
            mr = JSONUtil.parseObject(cacheMr,MsgRecordModel.class);
            if(mr != null){
                return mr;
            }
        }

        // 从数据库获取
        mr = msgRecordMapper.getMsgById(msgId);

        // 存入缓存
        redisTemplate.opsForValue().set(msgRecordCacheKey,JSONUtil.toJsonString(mr), Duration.ofSeconds(30));

        return mr;
    }
}
