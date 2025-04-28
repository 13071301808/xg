package com.yishou.bigdata.realtime.dw.common.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AlarmUtil {
    private boolean isShort = false;
    private String tag = "lark_md";
    private String alarmTitle;
    private String authorization;
    private List<JSONObject> alarmContent = new ArrayList<>();
    private String alarmUrl = "https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df";
    private String color = "red";

    public AlarmUtil(String alarmTitle) throws IOException {
        this.alarmTitle = alarmTitle;
//        this.authorization = getAuthorization();
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

    public Response sendFetshuQIYU(JSONObject alarmMessage) throws IOException {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS) // 设置连接超时时间为 30 秒
                .readTimeout(30, TimeUnit.SECONDS)    // 设置读取超时时间为 30 秒
                .writeTimeout(30, TimeUnit.SECONDS)   // 设置写入超时时间为 30 秒
                .build();

        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(mediaType, String.valueOf(alarmMessage));
        Request request = new Request.Builder()
                .url("http://192.168.2.168:8080/alarm/inDataPip")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .build();

        Response response = client.newCall(request).execute();
        return response;
    }
}
