package cn.bitoffer.msgcenter.consumer;

import cn.bitoffer.msgcenter.constant.Constants;
import cn.bitoffer.msgcenter.enums.MsgStatus;
import cn.bitoffer.msgcenter.enums.PriorityEnum;
import cn.bitoffer.msgcenter.manager.DealMsgManager;
import cn.bitoffer.msgcenter.mapper.MsgQueueMapper;
import cn.bitoffer.msgcenter.model.MsgQueueModel;
import cn.bitoffer.msgcenter.model.dto.SendMsgReq;
import cn.bitoffer.msgcenter.utils.JSONUtil;
import cn.bitoffer.msgcenter.utils.SQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class MysqlMsgConsumer {

    @Autowired
    MsgQueueMapper msgQueueMapper;

    @Autowired
    DealMsgManager dealMsgManager;


    @Scheduled(fixedRate = 1500)
    public void consumeLow() {
        consumeMySQLMsg(PriorityEnum.PRIORITY_LOW);
    }

    @Scheduled(fixedRate = 1000)
    public void consumeMiddle() {
        consumeMySQLMsg(PriorityEnum.PRIORITY_MIDDLE);
    }

    @Scheduled(fixedRate = 500)
    public void consumeHigh() {
        consumeMySQLMsg(PriorityEnum.PRIORITY_HIGH);
    }
    
    private void consumeMySQLMsg(PriorityEnum priority){

        // 1. 根据有限级确定表明
        String tableName = Constants.TableNamePre_MsgQueue+ PriorityEnum.GetPriorityStr(priority.getPriorty());

        // 2. 获取一批待处理消息
        List<MsgQueueModel> msgList = msgQueueMapper.getMsgsByStatus(tableName,MsgStatus.Pending.getStatus(),100);

        // 如果消息为空，则退出
        if(msgList == null || msgList.size() == 0){
            return;
        }

        // 4. 批量将msgList全部变为处理中
        List<String> msgIdList = msgList.stream()
                .map(MsgQueueModel::getMsgId)
                .collect(Collectors.toList());
        String msgIdListStr = SQLUtil.convertListToSQLString(msgIdList);
        msgQueueMapper.batchSetStatus(tableName,msgIdListStr,MsgStatus.Processiong.getStatus());

        // 5. 遍历处理这一批消息
        for (MsgQueueModel dbModel:msgList) {
            SendMsgReq req = new SendMsgReq();
            req.setMsgID(dbModel.getMsgId());
            req.setPriority(dbModel.getPriority());
            req.setTo(dbModel.getTo());
            req.setSubject(dbModel.getSubject());
            req.setTemplateId(dbModel.getTemplateId());

            Map<String,String> templateData = JSONUtil.parseMap(dbModel.getTemplateData(),String.class,String.class);
            req.setTemplateData(templateData);

            // 走消息发送逻辑
            try{
                dealMsgManager.DealOneMsg(req);
            }catch (Exception e){
                // 发送失败
                msgQueueMapper.setStatus(tableName,req.getMsgID(),MsgStatus.Failed.getStatus());
            }
            // 发送成功
            msgQueueMapper.setStatus(tableName,req.getMsgID(),MsgStatus.Succeed.getStatus());
        }

    }
}
