package cn.bitoffer.msgcenter.model;


import cn.bitoffer.common.model.BaseModel;

import java.io.Serializable;

/**
 * TemplateModel 消息模板
 *
 **/
public class MsgRecordModel extends BaseModel implements Serializable {

    private Long id;

    private String msgId;

    private String sourceId;

    private int channel;

    private String subject;

    private String to;

    private String templateId;

    private String templateData;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public int getChannel() {
        return channel;
    }

    public void setChannel(int channel) {
        this.channel = channel;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getTemplateId() {
        return templateId;
    }

    public void setTemplateId(String templateId) {
        this.templateId = templateId;
    }

    public String getTemplateData() {
        return templateData;
    }

    public void setTemplateData(String templateData) {
        this.templateData = templateData;
    }


}
