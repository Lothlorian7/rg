package cn.bitoffer.msgcenter.service;

import cn.bitoffer.msgcenter.model.MsgRecordModel;

public interface MsgRecordService {

    MsgRecordModel GetMsgRecordWithCache(String msgId);
}
