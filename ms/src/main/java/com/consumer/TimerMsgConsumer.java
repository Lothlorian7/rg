package cn.bitoffer.msgcenter.consumer;

import cn.bitoffer.msgcenter.conf.SendMsgConf;
import cn.bitoffer.msgcenter.constant.Constants;
import cn.bitoffer.msgcenter.enums.MsgStatus;
import cn.bitoffer.msgcenter.enums.PriorityEnum;
import cn.bitoffer.msgcenter.manager.DealMsgManager;
import cn.bitoffer.msgcenter.manager.SendMsgManager;
import cn.bitoffer.msgcenter.mapper.MsgQueueMapper;
import cn.bitoffer.msgcenter.mapper.MsgQueueTimerMapper;
import cn.bitoffer.msgcenter.model.MsgQueueModel;
import cn.bitoffer.msgcenter.model.MsgQueueTimerModel;
import cn.bitoffer.msgcenter.model.dto.SendMsgReq;
import cn.bitoffer.msgcenter.redis.MsgCache;
import cn.bitoffer.msgcenter.utils.JSONUtil;
import cn.bitoffer.msgcenter.utils.SQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class TimerMsgConsumer {

    @Autowired
    MsgQueueTimerMapper msgQueueTimerMapper;


    @Autowired
    SendMsgManager sendMsgManager;

    @Autowired
    SendMsgConf sendMsgConf;


    @Autowired
    MsgCache msgCache;


    @Scheduled(fixedRate = 100)
    public void consume() {
        consumeTimerMsgs();
    }
    
    private void consumeTimerMsgs(){
        // 1. 从Redis获取是否存在到点的 时间点
        List<String>  times = msgCache.getOnTimePointsFromCache();
        if(times == null || times.size() == 0){
            return;
        }

        // 根据缓存判读，当前时间点已经存在到点消息
        // 2.从数据库查询出具体到点的消息列表
        List<MsgQueueTimerModel> onTimeMsgs = msgQueueTimerMapper.getOnTimeMsgsList(MsgStatus.Pending.getStatus(), new Date().getTime());
        if(onTimeMsgs == null || onTimeMsgs.size() == 0){
            return;
        }

        // 3. 将msgList这里皮消息全部变为处理中Processiong
        List<String> msgIdList = onTimeMsgs.stream()
                .map(MsgQueueTimerModel::getMsgId)
                .collect(Collectors.toList());
        String msgIdListStr = SQLUtil.convertListToSQLString(msgIdList);
        msgQueueTimerMapper.batchSetStatus(msgIdListStr,MsgStatus.Processiong.getStatus());

        // 4. 遍历挨个处理到点消息
        for (MsgQueueTimerModel dbModel:onTimeMsgs) {
            SendMsgReq sendMsgReq = JSONUtil.parseObject(dbModel.getReq(),SendMsgReq.class);
            if (sendMsgReq == null){
                continue;
            }
            if(sendMsgConf.isMysqlAsMq()){
                // 发送到 Mysql
                sendMsgManager.SendToMysql(sendMsgReq);
            }
            // 发送到 MQ
            sendMsgManager.SendToMq(sendMsgReq);
        }

        // 5. 将msgList这里皮消息全部变为处理中Succeed
        msgQueueTimerMapper.batchSetStatus(msgIdListStr,MsgStatus.Succeed.getStatus());
    }
}
