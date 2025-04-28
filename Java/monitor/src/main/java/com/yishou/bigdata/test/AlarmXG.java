package com.yishou.bigdata.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class AlarmXG {
    static Logger logger = LoggerFactory.getLogger(AlarmXG.class);
    private boolean isShort = false;
    private String tag = "lark_md";
    private String alarmTitle;
    private String authorization;
    private List<JSONObject> alarmContent = new ArrayList<>();
    private String alarmUrl = "https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df";
    private String color = "red";

    public AlarmXG(String alarmTitle) {
        this.alarmTitle = alarmTitle;
    }

    public JSONObject generateAlarmMessage(List<JSONObject> alarmFieldMessage) {
        JSONObject card = new JSONObject();
        JSONObject config = new JSONObject();
        config.put("wide_screen_mode", true);
        card.put("config", config);

        JSONArray elements = new JSONArray();
        elements.add(new JSONObject().put("tag", "hr"));
        // 修正这里使用fluentPut保持链式调用
        elements.add(new JSONObject().fluentPut("fields", new JSONArray(alarmFieldMessage)).fluentPut("tag", "div"));

        card.put("elements", elements);
        // 修正header部分的链式调用
        card.put("header", new JSONObject()
                .fluentPut("template", color)
                .fluentPut("title", new JSONObject().fluentPut("content", alarmTitle).fluentPut("tag", "plain_text")));

        return new JSONObject().fluentPut("msg_type", "interactive").fluentPut("card", card);
    }

    // 设置飞书告警 url
    public void setAlarmUrl(String url) {
        this.alarmUrl = url;
    }

    // 设置飞书模板颜色
    public void setColor(String color) {
        this.color = color;
    }

    // 生成告警字段的信息
    public JSONObject getAlarmFieldInfo(String fieldName, String fieldValue) {
        JSONObject fieldInfo = new JSONObject();
        fieldInfo.put("is_short", isShort);
        fieldInfo.fluentPut("text", new JSONObject().fluentPut("content", String.format("**%s**: %s", fieldName, fieldValue)).fluentPut("tag", tag));
        return fieldInfo;
    }

    // 设置告警字段的信息
    public void setAlarmField(String fieldName, String fieldValue) {
        JSONObject fieldInfo = getAlarmFieldInfo(fieldName, fieldValue);
        alarmContent.add(fieldInfo);
    }

    // 生成告警信息
    public JSONObject buildAlarm() {
        return generateAlarmMessage(alarmContent);
    }

    // 发送到飞书
    public String sendFeishu(JSONObject alarmMessage) {
        try {
            URL url = new URL(alarmUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Bearer " + authorization);
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = alarmMessage.toString().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            return "Response Code: " + responseCode;

        } catch (Exception e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }

//    public static void main(String[] args) {
//        AlarmXG alarm = new AlarmXG("Alarm Title");
//        alarm.setAlarmField("告警字段", "java芜湖起飞");
//        alarm.setColor("green");
//        alarm.setAlarmUrl("https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df");
//        JSONObject alarmMessage = alarm.buildAlarm();
//        String response = alarm.sendFeishu(alarmMessage);
//        logger.info("开始调用{}",alarmMessage);
//
//    }

}
