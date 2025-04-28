package com.yishou.bigdata.test;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.RequestBody;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class posttest {
    public static void main(String[] args) throws IOException {
        // 实时埋点告警
        String alarm_log_concent = "{"
                + "\"msg_type\":\"interactive\","
                + "\"card\":{"
                +     "\"elements\":["
                +         "null,"
                +         "{"
                +             "\"tag\":\"div\","
                +             "\"fields\":["
                +                 "{"
                +                     "\"text\":{"
                +                         "\"tag\":\"lark_md\","
                +                         "\"content\":\"**日志库**: h5_log_store\""
                +                     "},"
                +                     "\"is_short\":false"
                +                 "},"
                +                 "{"
                +                     "\"text\":{"
                +                         "\"tag\":\"lark_md\","
                +                         "\"content\":\"**告警事件**: addcart\""
                +                     "},"
                +                     "\"is_short\":false"
                +                 "},"
                +                 "{"
                +                     "\"text\":{"
                +                         "\"tag\":\"lark_md\","
                +                         "\"content\":\"**告警字段**: add_cart_id\""
                +                     "},"
                +                     "\"is_short\":false"
                +                 "},"
                +                 "{"
                +                     "\"text\":{"
                +                         "\"tag\":\"lark_md\","
                +                         "\"content\":\"**告警规则**: 是否为空\""
                +                     "},"
                +                     "\"is_short\":false"
                +                 "}"
                +             "]"
                +         "}"
                +     "],"
                +     "\"header\":{"
                +         "\"template\":\"green\","
                +         "\"title\":{"
                +             "\"tag\":\"plain_text\","
                +             "\"content\":\"通用实时埋点告警-小程序加购\""
                +         "}"
                +     "},"
                +     "\"config\":{"
                +         "\"wide_screen_mode\":true"
                +     "}"
                + "}"
                + "}";

        // 算法告警
        String alarm_alarm_concent = "{"
                + "\"signature\":\"L/iarEuwT/ie65V1skbRbPgeiYqRwL1Eak5PlP+Zh3x/kr1e3qIdTK2DX8EDoZ/gzBmObs3jfv2ZjPL0q0j3HSummDdo636PeblvmIvtl2i/oi1aLQZaf+wI1WZyQRBAHYgI6/ytyGgpE6aci42v1Zer/VcSGa90BnGpquLgdugnwu2iXTV4F/IidR9Whn29W0eHHzlZrNjzj01te3sZp+mv5W5F+A8bzdJX0uLtZSOGmVEdYeTXmwnSWrryQaR25nbM6OaHbG1nKKdNNd4Hirfu+zS1fSRaCQJOOyaGVeOHXwDkcjlnkH52iUlnt+PCsil2dt9g8vPatrPch/ipXw==\","
                + "\"topic_urn\":\"urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:S_level_warning\","
                + "\"message_id\":\"b3444207721a40568af51278e8558508\","
                + "\"signature_version\":\"v1\","
                + "\"type\":\"Notification\","
                + "\"message\":\"规则名称：测试，请忽略;\\n"
                + "告警级别：不急;\\n"
                + "发生时间：不发生;\\n"
                + "告警源：没源;\\n"
                + "资源类型：测试;\\n"
                + "资源标识：  \\\\n 不标识 \\\\n;\\n"
                + "统计类型：无\\n"
                + "表达式：无;\\n"
                + "当前值: NA;\\n"
                + "统计周期：不统计;\\n"
                + "查看详情：https://console.huaweicloud.com/lts/?region=cn-south-1&locale=zh-cn#/cts/logEventsLeftMenu/events?groupId=790c913a-9dfe-4ff5-8f5a-942372033088&topicId=c1775769-2d4f-48ef-b24e-13e845d506b3&groupName=data&topicName=recommendation_api_servers_log&alarmRuleId=d33bbffe-453c-47a5-96ab-c674c0defbd8&chartIndex=0&selectDate=300000&fromAlarm=view&visualization=true&createTime=1739469248624;\","
                + "\"unsubscribe_url\":\"https://console.huaweicloud.com/smn/subscription/unsubscribe?region=cn-south-1&region_id=cn-south-1&subscription_urn=urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:S_level_warning:bd084e1618a14e60b96e0ca8414a9914\","
                + "\"signing_cert_url\":\"https://smn.cn-south-1.myhuaweicloud.com/smn/SMN_cn-south-1_3f97b396bf5f4ff0a6dd320acb38bb03.pem\","
                + "\"timestamp\":\"2025-02-13T17:55:09Z\""
                + "}";
        // dgc作业告警
        String alarm_dgc_concent = "{\"signature\":\"0L9dtIOvRRvLKKOSbWhY7ZhDmy0UryO7TD2loLWTxqYwiY9B82QTou5imQyP7Jb9f/1b/YlxqO8R1+NeEsybeyTOBhVeJGUDlgtOmDuV2r3AvtzZOClQLANFaO417hqQSl3nWObZ5N/UPTwzPgn88O0MnkVHG4MHEO/51+KFaxGM5Dcybpua0L/9TEDoKM6TdMSaxXlJ906DB0z9BmJTRSfVe0EA/iPLuC9h7/XfxMC+TIGpQZrj/kO+ErwqUDuBoFrdddzAx/pjqzbIXfn9+6MOqtFQqbV6+gKMvv4Xame7cOBhie8CIfgLITCW8bnfHgPngTkdUbENtYJPYC4G5w==\",\"subject\":\"DataArts数据开发作业 [ads_class_recall_for_area_province_cat_goods] 执行失败。\",\"topic_urn\":\"urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:yishou_daily\",\"message_id\":\"16bc50c8e9124c6da47e352e1e697ff1\",\"signature_version\":\"v1\",\"type\":\"Notification\",\"message\":\"{\\\"工作空间\\\":\\\"yishou_daily\\\",\\\"作业名称\\\":\\\"ads_class_recall_for_area_province_cat_goods\\\",\\\"作业执行人\\\":\\\"委托:dgc_agency\\\",\\\"最后修改人\\\":\\\"yangshibiao\\\",\\\"作业状态\\\":\\\"执行失败\\\",\\\"触发时间\\\":\\\"2025-02-14 17:00:00\\\",\\\"告警时间\\\":\\\"2025-02-14 17:10:05\\\",\\\"作业实例ID\\\":\\\"192674597\\\",\\\"调度类型\\\":\\\"正常调度\\\",\\\"失败节点名称\\\":\\\"ads_class_recall_for_area_province_cat_goods_pre_ht\\\",\\\"错误信息\\\":\\\"Please contact DLI service. DLI.0999: SparkException: Job aborted.\\\\nCause by: SparkException: Job aborted due to stage failure: Task 0 in stage 26324.0 failed 4 times, most recent failure: Lost task 0.3 in stage 26324.0 (TID 3173721, executor 677): java.io.FileNotFoundException: getFileStatus on obs://yishou-bigdata/yishou_data.db/dim_goods_no_info_full_h/part-00000-04a63cfa-b9fe-4420-8973-51f8f9f3ead5-c000: ResponseCode[404],ErrorCode[NoSuchKey],ErrorMessage[null\\\\nCause by: FileNotFoundException: getFileStatus on obs://yishou-bigdata/yishou_data.db/dim_goods_no_info_full_h/part-00000-04a63cfa-b9fe-4420-8973-51f8f9f3ead5-c000: ResponseCode[404],ErrorCode[NoSuchKey],ErrorMessage[null\\\\nRequest Error.],RequestId[0000019503B8A478964791AA54C16EB0]\\\\nCause by: ObsException: com.obs.services.exception.ObsException: Error message:Request Error.OBS service Error Message. -- ResponseCode: 404, ResponseStatus: Not Found, RequestId: 0000019503B8A478964791AA54C16EB0, HostId: 32AAAQAAEAABAAAQAAEAABAAAQAAEAABCSAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\\\",\\\"告警类型\\\":\\\"作业执行状态告警\\\"}\",\"unsubscribe_url\":\"https://console.huaweicloud.com/smn/subscription/unsubscribe?region=cn-south-1&region_id=cn-south-1&subscription_urn=urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:yishou_daily:5df66e7ff36d44f4bf9ec5024f4c4551\",\"signing_cert_url\":\"https://smn.cn-south-1.myhuaweicloud.com/smn/SMN_cn-south-1_3f97b396bf5f4ff0a6dd320acb38bb03.pem\",\"timestamp\":\"2025-02-14T09:10:14Z\"}";


        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS) // 设置连接超时时间为 30 秒
                .readTimeout(30, TimeUnit.SECONDS)    // 设置读取超时时间为 30 秒
                .writeTimeout(30, TimeUnit.SECONDS)   // 设置写入超时时间为 30 秒
                .build();

        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(mediaType, alarm_dgc_concent);
        Request request = new Request.Builder()
                .url("http://139.159.160.115:8080/alarm/inDataPip")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .build();

        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());
    }
}
