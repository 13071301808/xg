package com.fmys.api.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fmys.api.domain.WarnMessage;
import com.fmys.api.domain.hw.dgc.DgcInstance;
import com.fmys.api.mapper.WarnMessageMapper;
import com.fmys.api.service.WarnMessageService;

import com.huawei.dli.restapi.model.GetFlinkJobDetailResponse;
import com.huawei.dli.sdk.DLIClient;
import com.huawei.dli.sdk.exception.DLIException;
import okhttp3.*;
import org.apache.shiro.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


@Service
public class WarnMessageServiceImpl implements WarnMessageService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //  2022/5/9/0009 1，从数据库动态加载
    @Value("${yishou.warning.feishuUrl}")
    String feishuUrl ;

    @Resource
    private WarnMessageMapper warnMessageMapper;

    @Autowired
    @Qualifier("okHttpClient")
    private OkHttpClient okHttpClient;

    @Autowired
    @Qualifier(value = "dliClient")
    private DLIClient dliClient;

    @Value("${huawei.dli.domain.region}")
    private String region;

    @Value("${huawei.dli.domain.projectId}")
    private String projectId;


    // 1.从数据库中查作业是否为重保作业，来发送不同告警样式
    // 2.修改作业执行人或者作业创建者，从数据库中拿
    @Override
    public void addRecord(String record){
        ArrayList<String> urls = new ArrayList<>();
        urls.add(feishuUrl);
        logger.info("完整告警信息"+record);
        WarnMessage warnMessage=null;
        try {
            warnMessage = parseWarnMessage(record);
//            String token = dliClient.getAuthToken();
//            String failNode = getFailNode(warnMessage, token);
//            warnMessage.setFailNodeName(failNode);
            String warnStyle = warnStyle4Topics(warnMessage);
            // 从数据库获取robotUrl，可能发送多个群
            String robotUrl ;
            if("DGC数据开发作业".equals(warnMessage.getJobType())){
                robotUrl = warnMessageMapper.getDGCFeishuRobotUrl(warnMessage.getJobName(), warnMessage.getWorkSpace());
            }else if (warnMessage.getJobTypeId()==6){
                robotUrl = warnMessageMapper.getDGCFeishuRobotUrl("算法", "算法");
            }else if (warnMessage.getJobTypeId()==7) {
                JSONObject json_concent = JSONObject.parseObject(record);
                // 提取 card 对象
                String job_name = json_concent.getJSONObject("card")
                        .getJSONObject("header")
                        .getString("title");
                robotUrl = warnMessageMapper.getFlinkLogFeishuRobotUrl(job_name);
            }
            else {
                robotUrl = warnMessageMapper.getFlinkFeishuRobotUrl(warnMessage.getJobId());
            }
            if(null != robotUrl){
                urls.clear();
                String[] urls1 = robotUrl.split(",");
                for (String s : urls1) {
                    urls.add(s);
                }
            }
            if(Objects.equals(warnStyle, "实时埋点信息")){
                send("",record,urls);
            }else {
                send(warnStyle,messageFormat(warnMessage),urls);
            }
        }catch (Exception e){
            logger.info("未知告警格式");
//            warnStyle4Topics(warnMessage);
//            warnMessage.setWarnTime(new Date());
            send("",record,urls);
        }finally {
            warnMessageMapper.saveToDatabase(warnMessage);
        }
    }

    /**
     * 解析告警信息，
     * @param record
     * @return warnMessage
     */
    public WarnMessage parseWarnMessage(String record) throws DLIException {
        Calendar calendar = Calendar.getInstance();
        String authToken="";
        WarnMessage warnMessage = new WarnMessage();

        try {
            authToken = dliClient.getAuthToken();
        } catch (DLIException e) {
            e.printStackTrace();
        }
        logger.info("封装告警信息对象");
        warnMessage.setRecInfo(record);
        warnMessage.setIsUrgency(0);
        try {
            JSONObject jsonObject = JSONObject.parseObject(record);
            String subject = jsonObject.getString("subject");
            if (subject == null){
                subject = "";
            }
            String message = jsonObject.getString("message");
            if (message == null){
                message = "";
            }

            logger.info("message: "+message);
            logger.info("subject: "+subject);

            // flink 作业告警

            if (subject.contains("数据湖探索_作业异常")) {
                int i = subject.indexOf("：");
                int flinkJobId = Integer.parseInt(subject.substring(i + 1, subject.length() - 1));
                logger.info("subject: "+message);
                warnMessage.setJobId(flinkJobId);
                warnMessage.setJobName(subject.split(">")[0].substring(1));
                warnMessage.setJobType("数据湖探索");
                warnMessage.setJobTypeId(1);
                warnMessage.setProcessorDocUrl(warnMessageMapper.getFlinkProcessorDocUrl(warnMessage.getJobId()));
                calendar.setTime(jsonObject.getDate("timestamp"));
                // 转化时区为+8h
                calendar.add(Calendar.HOUR_OF_DAY, 0);
                Date time = calendar.getTime();
                warnMessage.setWarnTime(time);
                warnMessage.setTopic(jsonObject.getString("topic_urn"));
                GetFlinkJobDetailResponse flinkJobDetail;
                try {
                    flinkJobDetail = dliClient.getFlinkJobDetail((long) flinkJobId);
                    String userId = flinkJobDetail.getJobDetail().getUserId();
                    String userName = warnMessageMapper.getUserName(userId);
                    warnMessage.setLastUpdateUser(userName);
                    warnMessage.setSecondType(flinkJobDetail.getJobDetail().getJobType());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            //表示作业为 DGC数据开发作业
            else if (subject.indexOf("DataArts数据开发作业") > -1) {
                JSONObject dgcMessage = JSONObject.parseObject(message.replaceAll("\\\"", "\""));
                // 动态获取所需告警字段,赋值给对象所有属性
                warnMessage.setJobId(Integer.parseInt(dgcMessage.getString("作业实例ID")));
                warnMessage.setJobName(dgcMessage.getString("作业名称"));
                warnMessage.setJobType("DGC数据开发作业");
                warnMessage.setJobTypeId(2);
                warnMessage.setWorkSpace(dgcMessage.getString("工作空间"));
                warnMessage.setDispatchType(dgcMessage.getString("调度类型"));
                warnMessage.setJobStatus(dgcMessage.getString("作业状态"));
                warnMessage.setExecutor(dgcMessage.getString("作业执行人"));
                warnMessage.setWarnType(dgcMessage.getString("告警类型"));
                String failNode = getFailNode(warnMessage, authToken);
                warnMessage.setFailNodeName(failNode);
                String updateUser = warnMessageMapper.getLastUpdateUser(warnMessage.getJobName(),warnMessage.getWorkSpace());
                Integer isUrgency = warnMessageMapper.isUrgency(warnMessage.getJobName(),warnMessage.getWorkSpace());
                if (null != updateUser){
                    warnMessage.setLastUpdateUser(updateUser);
                }else {
                    warnMessage.setLastUpdateUser(dgcMessage.getString("最后修改人"));
                }
                warnMessage.setProcessorDocUrl(warnMessageMapper.getDGCProcessorDocUrl(warnMessage.getJobName(),warnMessage.getWorkSpace()));
                warnMessage.setTopic(jsonObject.getString("topic_urn"));
                if(null != isUrgency){
                    warnMessage.setIsUrgency(warnMessageMapper.isUrgency(warnMessage.getJobName(),warnMessage.getWorkSpace()));
                }

                try {
                    warnMessage.setWarnTime(simpleDateFormat.parse(dgcMessage.getString("告警时间")));
                    warnMessage.setTriggerTime(simpleDateFormat.parse(dgcMessage.getString("触发时间")));
                } catch (ParseException parseException) {
                    parseException.printStackTrace();
                }
                Request dgcJobRequest = new Request.Builder()
                        .url("https://dayu-dlf.cn-south-1.myhuaweicloud.com/v1/0ba6e278cc80f3562f48c00242b9c5d8/jobs/" + warnMessage.getJobName())
                        .get()
                        .header("X-Auth-Token", authToken)
                        .header("Content-Type", "application/json")
                        .header("workspace", warnMessageMapper.getWorkspaceId(warnMessage.getWorkSpace()))
                        .build();
                //获取dgc作业类型
                try {
                    Response response = okHttpClient.newCall(dgcJobRequest).execute();
                    JSONArray nodes = JSONObject.parseObject(response.body().string()).getJSONArray("nodes");
                    Iterator<Object> iterator = nodes.iterator();
                    while (iterator.hasNext()) {
                        JSONObject jobInfo = JSONObject.parseObject(iterator.next().toString());
                        if (warnMessage.getJobName().equals(jobInfo.getString("name"))) {
                            warnMessage.setSecondType(jobInfo.getString("type"));
                        }
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    logger.info(e.getMessage());
                }
            }else if(subject.contains("分布式消息服务")) {
                JSONObject message1 = jsonObject.getJSONObject("message");
                JSONObject template_variable = message1.getJSONObject("template_variable");
                warnMessage.setJobType("分布式消息服务");
                warnMessage.setJobName(subject);
                warnMessage.setTopic(message1.getString("dimension").split(",")[2].split(":")[1]);
                warnMessage.setConsumerGroup(message1.getString("dimension").split(",")[1].split(":")[1]);
                if (subject.contains("已恢复正常")) {
                    warnMessage.setJobStatus("已恢复正常");
                }
                warnMessage.setJobTypeId(4);
                warnMessage.setMetric(template_variable.getString("MetricName"));
                warnMessage.setValue(template_variable.getString("CurrentData"));
                warnMessage.setWarnTime(new Date(System.currentTimeMillis()));
            }else if(subject.contains("数据湖探索-队列")){
                JSONObject messageObj = JSONObject.parseObject(message);
                // [华南-广州][重要告警恢复]尊敬的hw14505396：数据湖探索-队列 “develop” （ID：91040）的提交中作业数当前数据：0.00Count，于2022/09/13 08:09:21 GMT+08:00恢复正常，触发规则：Spark-Alters，详情请访问云监控服务。
                String sms_content = messageObj.getString("sms_content");
                String time = messageObj.getString("time");
                warnMessage.setJobTypeId(5);
                warnMessage.setJobName(subject);
                if(subject.contains("恢复")){
                    warnMessage.setJobStatus("已恢复正常");
                    warnMessage.setWarnTime(jsonObject.getDate("timestamp"));
                }else{
                    warnMessage.setJobStatus("触发告警");
                    String[] s = sms_content.split("：");
                    warnMessage.setMetric(s[1]+s[2]+s[3]);
                }
            }else if(message.contains("规则名称")){
                subject = message.split("规则名称：")[1].split(";")[0];
                warnMessage.setJobTypeId(6);
                warnMessage.setJobName(subject);
                warnMessage.setMetric(message);
            } else if (jsonObject.getJSONObject("card").getJSONObject("header").getString("title").contains("通用实时埋点告警")) {
                String content = jsonObject.getJSONObject("card").getJSONObject("header").getString("title");
                warnMessage.setJobTypeId(7);
                warnMessage.setJobType("实时埋点告警");
                warnMessage.setJobName(content);
                warnMessage.setWarnTime(new Date(System.currentTimeMillis()));
            } else {
                warnMessage.setJobTypeId(3);
                warnMessage.setWarnTime(new Date(System.currentTimeMillis()));
            }
        }catch (Exception e){
//            e.printStackTrace();
            logger.info("告警信息解析失败");
        }
        return warnMessage;
    }

    /**
     * 拼接告警样式Json
     * 效果：按照不同的topic，发送不同等级的告警样式头部
     * @param warnMessage
     * @return
     */
    public String warnStyle4Topics(WarnMessage warnMessage){
//        String yishouDataWarningTopic = "urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:yishou_data";
        String aLevelWarningTopic = "urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:A_level_warning";
        String sLevelWarningTopic = "urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:S_level_warning";
        String flinkTopic = "urn:smn:cn-south-1:0ba6e278cc80f3562f48c00242b9c5d8:flink_monitor";
        StringBuffer stringBuffer = new StringBuffer();

        //按照不同作业类型定义告警信息结构,如果是已知的消息格式，进行格式化
        // 否则将整个消息体发送出去
//        if(null != warnMessage.getJobId() || null != warnMessage.getJobName()) {
//            String messageFormat = messageFormat(warnMessage);
//            stringBuffer.append(messageFormat);
//        }else {
//            stringBuffer.append(warnMessage.getRecInfo());
//            return stringBuffer.toString();
//        }

        if("执行成功".equals(warnMessage.getJobStatus()) || "已恢复正常".equals(warnMessage.getJobStatus())){
            stringBuffer.append(
                    "              \"header\": {\n" +
                            "                \"template\": \"green\",\n" +
                            "                \"title\": {\n" +
                            "                  \"content\": \"\uD83E\uDD42"+warnMessage.getJobName()+"\uD83E\uDD42\",\n" +
                            "                  \"tag\": \"plain_text\"\n" +
                            "                }\n" +
                            "              }\n" +
                            "            }}");

        }
        else if(1 == warnMessage.getIsUrgency()){
            stringBuffer.append(
                    "              \"header\": {\n" +
                            "                \"template\": \"red\",\n" +
                            "                \"title\": {\n" +
                            "                  \"content\": \"\uD83D\uDD25紧急！DGC重保作业\uD83D\uDD25\",\n" +
                            "                  \"tag\": \"plain_text\"\n" +
                            "                }\n" +
                            "              }\n" +
                            "            }}");
        }
        else if(warnMessage.getJobName().contains("云监控通知") || flinkTopic.equals(warnMessage.getTopic()) || 6 == warnMessage.getJobTypeId()){
            // 包含kafka、dli队列、flink作业、算法
            stringBuffer.append(
                    "              \"header\": {\n" +
                            "                \"template\": \"red\",\n" +
                            "                \"title\": {\n" +
                            "                  \"content\": \""+warnMessage.getJobName()+"\",\n" +
                            "                  \"tag\": \"plain_text\"\n" +
                            "                }\n" +
                            "              }\n" +
                            "            }}");
        } else if (warnMessage.getJobName().contains("通用实时埋点告警") || 7 == warnMessage.getJobTypeId()) {
            logger.info("请求头为实时埋点信息");
            stringBuffer.append("实时埋点信息");
        } else {        //a_level
            stringBuffer.append(
                    "              \"header\": {\n" +
                            "                \"template\": \"orange\",\n" +
                            "                \"title\": {\n" +
                            "                  \"content\": \""+warnMessage.getJobName()+"\",\n" +
                            "                  \"tag\": \"plain_text\"\n" +
                            "                }\n" +
                            "              }\n" +
                            "            }}");
        }
        return stringBuffer.toString();
    }

    /**
     * 对dgc作业和flink作业告警信息进行格式化
     * 将其他格式的告警信息整个作为消息体
     * @param warnMessage
     * @return
     */
    private String messageFormat(WarnMessage warnMessage) {

        //dgc作业告警信息
        if(2 == warnMessage.getJobTypeId()) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(
                    "              \"elements\": [\n" +
                            "                {\n" +
                            "                  \"tag\": \"hr\"\n" +
                            "                },\n" +
                            "                {\n" +
                            "                  \"fields\": [\n" +

                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"作业实例ID: " + warnMessage.getJobId() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"作业名称:" + warnMessage.getJobName() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"失败节点:" + warnMessage.getFailNodeName() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"工作空间: " + warnMessage.getWorkSpace() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"调度类型: " + warnMessage.getDispatchType() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"作业状态: " + warnMessage.getJobStatus() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"作业执行人: " + warnMessage.getExecutor() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"最后修改人: " + warnMessage.getLastUpdateUser() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"告警类型: " + warnMessage.getWarnType() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"告警时间: " + simpleDateFormat.format(warnMessage.getWarnTime()) + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"触发时间: " + simpleDateFormat.format(warnMessage.getTriggerTime()) + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"处理参考文档: " + warnMessage.getProcessorDocUrl() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    }\n" +

                            "                  ],\n" +
                            "                  \"tag\": \"div\"\n" +
                            "                }\n" +
                            "              ],\n"
            );
            return stringBuffer.toString();
            //flink作业告警信息
        }else if(1 == warnMessage.getJobTypeId()){
            return
                    "              \"elements\": [\n" +
                            "                {\n" +
                            "                  \"tag\": \"hr\"\n" +
                            "                },\n" +
                            "                {\n" +
                            "                  \"fields\": [\n" +
                            "                   {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**作业名称**: "+warnMessage.getJobName()+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**负责人**: "+warnMessage.getLastUpdateUser()+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**告警时间**: "+simpleDateFormat.format(warnMessage.getWarnTime())+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"处理参考文档: " + warnMessage.getProcessorDocUrl() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    }\n" +

                            "                  ],\n" +
                            "                  \"tag\": \"div\"\n" +
                            "                }\n" +
                            "              ],\n";

            //kafka
        }else if(4 == warnMessage.getJobTypeId()){
            return
                    "              \"elements\": [\n" +
                            "                {\n" +
                            "                  \"tag\": \"hr\"\n" +
                            "                },\n" +
                            "                {\n" +
                            "                  \"fields\": [\n" +
                            "{\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**作业名称**: "+warnMessage.getJobName().split(",")[0]+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**指标名**: "+warnMessage.getMetric()+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**当前值**: "+warnMessage.getValue()+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**topic**: "+warnMessage.getTopic()+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**消费者组**: "+warnMessage.getConsumerGroup()+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**告警时间**: "+simpleDateFormat.format(System.currentTimeMillis())+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"处理参考文档: " + warnMessage.getProcessorDocUrl() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    }\n" +
                            "                  ],\n" +
                            "                  \"tag\": \"div\"\n" +
                            "                }\n" +
                            "              ],\n";
        }else if(5 == warnMessage.getJobTypeId()){
            // dli队列告警
            return
                    "              \"elements\": [\n" +
                            "                {\n" +
                            "                  \"tag\": \"hr\"\n" +
                            "                },\n" +
                            "                {\n" +
                            "                  \"fields\": [\n" +

                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**指标内容**: "+warnMessage.getMetric()+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**告警时间**: "+simpleDateFormat.format(System.currentTimeMillis())+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"处理参考文档: " + warnMessage.getProcessorDocUrl() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    }\n" +
                            "                  ],\n" +
                            "                  \"tag\": \"div\"\n" +
                            "                }\n" +
                            "              ],\n";

        }
        else if(6 == warnMessage.getJobTypeId()){
            // 算法告警
            return " \"elements\": [\n" +
                    "                {\n" +
                    "                  \"tag\": \"hr\"\n" +
                    "                },\n" +
                    "                {\n" +
                    "                  \"fields\": [\n" +

                    "                    {\n" +
                    "                      \"is_short\": false,\n" +
                    "                      \"text\": {\n" +
                    "                        \"content\": \"**"+warnMessage.getMetric().replace("\"","\\\"").replace("\n","\\n").replaceFirst("查看详情.*?;","查看详情").replace("\\n查看详情","")+"**\",\n" +
                    "                        \"tag\": \"lark_md\"\n" +
                    "                      }\n" +
                    "                    },"+
                    "                    {\n" +
                    "                      \"is_short\": false,\n" +
                    "                      \"text\": {\n" +
                    "                        \"content\": \"<at id=all></at>**告警时间: "+simpleDateFormat.format(System.currentTimeMillis())+"**\",\n" +
                    "                        \"tag\": \"lark_md\"\n" +
                    "                      }\n" +
                    "                    }\n"+
                    "                  ],\n" +
                    "                  \"tag\": \"div\"\n" +
                    "                },\n" +
                    "                            {\n" +
                    "                            \"actions\": [{\n" +
                    "                                    \"tag\": \"button\",\n" +
                    "                                    \"text\": {\n" +
                    "                                            \"content\": \"**查看详情**\",\n" +
                    "                                            \"tag\": \"lark_md\"\n" +
                    "                                    },\n" +
                    "                                    \"url\": \""+warnMessage.getMetric().split("查看详情：")[1].split(";")[0]+"\",\n" +
                    "                                    \"type\": \"default\",\n" +
                    "                                    \"value\": {}\n" +
                    "                            }],\n" +
                    "                            \"tag\": \"action\"\n" +
                    "                    }"+
                    "              ],\n";

        } else if (7 == warnMessage.getJobTypeId()) {
            logger.info("无需重构格式");
            return "实时埋点告警";
        } else{     //其他
            String record = JSONObject.parseObject(warnMessage.getRecInfo()).getJSONArray("record").get(0).toString();
            String subject = JSONObject.parseObject(record).getJSONObject("smn").getString("subject");
            return
                    "              \"elements\": [\n" +
                            "                {\n" +
                            "                  \"tag\": \"hr\"\n" +
                            "                },\n" +
                            "                {\n" +
                            "                  \"fields\": [\n" +
                            "{\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**告警作业**: "+subject+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n" +
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"**告警时间**: "+simpleDateFormat.format(System.currentTimeMillis())+"\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    },\n"+
                            "                    {\n" +
                            "                      \"is_short\": false,\n" +
                            "                      \"text\": {\n" +
                            "                        \"content\": \"处理参考文档: " + warnMessage.getProcessorDocUrl() + "\",\n" +
                            "                        \"tag\": \"lark_md\"\n" +
                            "                      }\n" +
                            "                    }\n" +

                            "                  ],\n" +
                            "                  \"tag\": \"div\"\n" +
                            "                }\n" +
                            "              ],\n";

        }
    }

    //发送任何格式的数据到指定告警群
    private void send(String header,String warnTypeInfo, ArrayList<String> urls) {
        StringBuffer stringBuffer = new StringBuffer();
        // 要搜索的关键词
        String keyword = "通用实时埋点告警";
        // 进行模糊搜索
        boolean found = warnTypeInfo.contains(keyword);
        // 输出结果
        if (found) {
            logger.info("走实时埋点告警,不处理");
            stringBuffer.append(warnTypeInfo);
            Request request;
            int i = 0;
            while(i < urls.size()){
                try {
                    request = new Request.Builder()
                            .url(urls.get(i))
                            .post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"), stringBuffer.toString()))
                            .build();
                    Response response = this.okHttpClient.newCall(request).execute();
                    if(response.code() == 200){
                        logger.info(response.message()+"发送成功");
                    }else{
                        logger.info(response.message()+"发送失败");
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    logger.error(e.getMessage(),e);
                }
                i++;
            }
        }else {
            stringBuffer.append(
                    "{\n" +
                            "            \"msg_type\": \"interactive\",\n" +
                            "            \"card\": {\n" +
                            "              \"config\": {\n" +
                            "                \"wide_screen_mode\": true\n" +
                            "              },\n" );

            if(header.length()>1){
                stringBuffer.append(warnTypeInfo);
                stringBuffer.append(header);
            }else {
                // 走通用识别错误告警
                String str = warnTypeInfo.replaceAll("\\\"|\\\\","");
                stringBuffer.append(
                        "        \"elements\": [\n" +
                                "            {\n" +
                                "                \"tag\": \"hr\"\n" +
                                "            },\n" +
                                "            {\n" +
                                "                \"fields\": [\n" +
                                "                    {\n" +
                                "                        \"is_short\": false,\n" +
                                "                        \"text\": {\n" +
                                "                            \"content\": \""+str+"\" ,\n" +
                                "                            \"tag\": \"lark_md\"\n" +
                                "                        }\n" +
                                "                    },\n" +
                                "                    {\n" +
                                "                        \"is_short\": false,\n" +
                                "                        \"text\": {\n" +
                                "                        \"content\": \"**告警时间**: "+simpleDateFormat.format(System.currentTimeMillis())+"\",\n" +
                                "                            \"tag\": \"lark_md\"\n" +
                                "                        }\n" +
                                "                    }\n" +
                                "                ],\n" +
                                "                \"tag\": \"div\"\n" +
                                "            }\n" +
                                "        ],"+
                                "              \"header\": {\n" +
                                "                \"template\": \"orange\",\n" +
                                "                \"title\": {\n" +
                                "                  \"content\": \"信息解析失败，请查看具体内容\",\n" +
                                "                  \"tag\": \"plain_text\"\n" +
                                "                }\n" +
                                "              }\n" +
                                "            }}");


            }
            logger.info("告警样式："+stringBuffer.toString().replaceAll("[\\t\\n\\r]",""));

            Request request;
            int i = 0;
            while(i < urls.size()){
                try {
                    request = new Request.Builder()
                            .url(urls.get(i))
                            .post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"), stringBuffer.toString()))
                            .build();
                    Response response = this.okHttpClient.newCall(request).execute();
                    response.body().string();
                    if(response.code() == 200){
                        logger.info(response.message()+"发送成功");
                    }else{
                        logger.info(response.message()+"发送失败");
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    logger.error(e.getMessage(),e);
                }
                i++;
            }
        }
    }

    /**
     * 查询失败实例
     */
    public String getFailInstance(String workspace,String jobName,Long triggerTime,String token){
        String instanceListUrl = String.format("https://dayu-dlf.%s.myhuaweicloud.com/v1/%s/jobs/instances/detail?jobName=%s&status=fail&minPlanTime=%s"
                , region, projectId, jobName,triggerTime);
        logger.info(instanceListUrl);
        Response response;
        String resBody;
        JSONObject instanceList;
        DgcInstance dgcInstance = null;
        Request.Builder builder = new Request.Builder();
        //从接口返回当前报错的实例id
        Request instanceListBuild = builder.get().url(instanceListUrl)
                .header("x-auth-token", token)
                .header("workspace", warnMessageMapper.getWorkspaceId(workspace))
                .build();
        try {
            response = okHttpClient.newCall(instanceListBuild).execute();
            resBody = response.body().string();
            logger.info("实例列表：" + resBody);
            instanceList = JSONObject.parseObject(resBody);
            //获取最新的失败实例
            if (instanceList.getInteger("total") > 1) {
                JSONArray instances = instanceList.getJSONArray("instances");
                Long latestTime;
                for (int i = 0; i < instances.size() - 1; i++) {
                    Long endTime = instances.getJSONObject(i + 1).getLong("endTime");
                    latestTime = instances.getJSONObject(i).getLong("endTime");
                    if (latestTime > endTime) {
//                    instanceId = instances.getJSONObject(i).getLong("instanceId");
                        dgcInstance = new DgcInstance(instances.getString(i));
                    } else {
                        dgcInstance = new DgcInstance(instances.getString(i+1));
                    }
                    i++;
                }
                logger.info("查询作业实例成功:"+dgcInstance.toString());
                return dgcInstance.getInstanceId();
            } else if(instanceList.getInteger("total") == 1) {
                dgcInstance = new DgcInstance(instanceList.getJSONArray("instances").getString(0));
                logger.info("查询作业实例成功:"+dgcInstance.toString());
                return dgcInstance.getInstanceId();
            }else{
                logger.error("未获取到失败实例");
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * 查询失败节点
     */
    public String getFailNode(WarnMessage warnMessage,String token) {
        OkHttpClient okHttpClient = new OkHttpClient();
        Response response;
//        String instanceId = getFailInstance(warnMessage.getWorkSpace(), warnMessage.getJobName(), warnMessage.getTriggerTime().getTime() - 24 * 3600, token);
        Integer instanceId = warnMessage.getJobId();
        //查询单个实例详细信息
        String instanceUrl = String.format("https://dayu-dlf.%s.myhuaweicloud.com/v1/%s/jobs/%s/instances/%s"
                ,region,projectId,warnMessage.getJobName(),instanceId);
        logger.info("失败实例url："+instanceUrl);
        Request instanceBuild = new Request.Builder()
                .get()
                .url(instanceUrl)
                .header("x-auth-token",token)
                .header("workspace",warnMessageMapper.getWorkspaceId(warnMessage.getWorkSpace()))
                .build();
        String failInstance;
        JSONArray nodes;
        String failNode = "";
        try {
            response = okHttpClient.newCall(instanceBuild).execute();
            failInstance = response.body().string();
            nodes = JSONObject.parseObject(failInstance).getJSONArray("nodes");
            logger.info(nodes.toJSONString());
            Assert.notNull(nodes);
            for (int i = 0; i < nodes.size(); i++) {
                if("fail".equals(nodes.getJSONObject(i).getString("status"))){
                    failNode = nodes.getJSONObject(i).getString("nodeName");
                    break;
                }
            }
            logger.info("获取失败节点成功:"+failNode);
            return failNode;
        } catch (Exception e) {
            logger.info("获取失败节点失败");
            e.printStackTrace();
            return "未查询到失败节点";
        }
    }
}
