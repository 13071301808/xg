package com.yishou.bigdata.realtime.dw.common.utils;

import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @date: 2021/6/23
 * @author: yangshibiao
 * @desc: Http工具类
 */
public class OkHttpUtil {

    public static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .connectTimeout(60, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .writeTimeout(60, TimeUnit.SECONDS)
            .build();

    public static Object send(String url, String data) throws IOException {
        MediaType type = MediaType.parse("application/json; charset=utf-8");
        final Request request = new Request.Builder().url(url)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(type, data))
                .build();

        final Response resp = CLIENT.newCall(request).execute();
        return resp.code();
    }

}
