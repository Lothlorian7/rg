package cn.bitoffer.msgcenter.mapper;


import cn.bitoffer.msgcenter.model.MsgRecordModel;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface MsgRecordMapper {

    void save(@Param("msgRecordModel") MsgRecordModel msgRecordModel);

    MsgRecordModel getMsgById(@Param("msgId") String msgId);
}
