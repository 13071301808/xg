package com.yishou.bigdata.realtime.dw.common.basic;

import java.util.Map;

/**
 * 一手事件解析统接口
 */
public interface IAppService {

    /**
     * 此作业的核心业务逻辑
     *
     * @param configMap 配置对象
     */
    void process(Map<String, String> configMap);


}
