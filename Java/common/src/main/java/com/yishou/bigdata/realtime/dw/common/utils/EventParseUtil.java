package com.yishou.bigdata.realtime.dw.common.utils;

import com.alibaba.fastjson.JSONObject;

/**
 * @date: 2022/12/17
 * @author: yangshibiao
 * @desc: 事件解析工具类（对流量日志进行解析时的共有操作）
 */
public class EventParseUtil {

    /**
     * 根据传入的数据和事件名称，判断该数据是否属于该事件
     *
     * @param jsonText  流量数据
     * @param eventName 事件名称
     * @return boolean（true：属于，false：不属于）
     */
    public static boolean isEvent(JSONObject jsonText, String eventName) {

        return jsonText != null &&
                (
                        eventName.equalsIgnoreCase(jsonText.getString("topic"))
                                || eventName.equalsIgnoreCase(jsonText.getString("__topic__"))
                );

    }

}
