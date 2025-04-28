package com.fmys.api.mapper;

import com.fmys.api.domain.WarnMessage;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface WarnMessageMapper {
    void saveToDatabase(@Param("record") WarnMessage record);

    //  新建表：存储飞书告警群url 和作业名关系
    String getUserName(@Param("userId") String userId);

    String getWorkspaceId(@Param("workspace") String workspace);

    String getFlinkProcessorDocUrl(@Param("jobId") Integer jobId);

    String getDGCProcessorDocUrl(@Param("jobName") String jobName,@Param("workspace") String workspace);

    String getFlinkFeishuRobotUrl(@Param("jobId") Integer jobId);

    String getFlinkLogFeishuRobotUrl(@Param("jobName") String jobName);

    String getDGCFeishuRobotUrl(@Param("jobName") String jobName,@Param("workspace") String workspace);

    String getLastUpdateUser(@Param("jobName") String jobName, @Param("workspace") String workspace);

    Integer isUrgency(@Param("jobName") String jobName, @Param("workspace") String workspace);

}
