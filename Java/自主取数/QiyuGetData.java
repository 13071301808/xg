/*
    该脚本用于回刷从七鱼拉数到数仓的底层表：yishou_data.ods_hw_qiyu_user_session_d 
    数仓存储路径：obs://yishou-bigdata/yishou_data.db/qiyu_data/ods_qiyu_user_session_d
    数仓埋点解析脚本：清洗七鱼会话数据
    随笔：checkFileResult：{"code":14403,"message":"Wait..."}并不是错误码，这个是等待结果正在计算的意思
*/
@PostConstruct
public void ods_qiyu_user_session_user_message_d() throws NoSuchAlgorithmException, IOException, InterruptedException, ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    // 指定开始时间
    String startDateStr = "20250301"
    // 指定结束时间
    String endDateStr = "20250313"

    // 1. 解析日期参数
    Date startDate = sdf.parse(startDateStr);
    Date endDate = sdf.parse(endDateStr);
    DateTime currentDay = new DateTime(startDate);
    DateTime finalDay = new DateTime(endDate).plusDays(1); // 包含结束日

    // 2. 校验日期顺序
    if (currentDay.isAfter(finalDay)) {
        logger.error("开始日期不能晚于结束日期");
        return;
    }

    // 3. 循环处理每一天
    while (currentDay.isBefore(finalDay)) {
        String processDateStr = sdf.format(currentDay.toDate());
        logger.info("正在处理日期：{}", processDateStr);

        String dt = DateFormatUtils.format(DateUtils.addDays(new Date(), -6), "yyyyMMdd");
        Date date = sdf.parse(dt);

        // 生成当天的开始结束时间
        DateTime dateTime = currentDay
            .withHourOfDay(0)
            .withMinuteOfHour(0)
            .withSecondOfMinute(0);
        long start = dateTime.getMillis();
        logger.info("开始时间:{}",dateTime);
        DateTime endDateTime = currentDay
            .withHourOfDay(23)
            .withMinuteOfHour(59)
            .withSecondOfMinute(59);
        long end = endDateTime.getMillis();
        logger.info("结束时间:{}",endDateTime);

        long millis = Calendar.getInstance().getTimeInMillis();
        Long time = millis / 1000;

        String paramsJson = "{\"start\":\"" + start + "\",\"end\":\"" + end + "\"}";
        String checkSum = QiYuCommonUtils.genCheckSum(this.qiyu_encrypt_key, time, paramsJson);
        String sessionUrl = String.format(this.session_url, this.app_key, time, checkSum);
        logger.info("会话连接:{}",sessionUrl);
        Request request = new Request.Builder()
            .url(sessionUrl)
            .post(RequestBody.create(MediaType.parse("application/json; charset=utf-8"), paramsJson))
            .build();
        Response response = okHttpClient.newCall(request).execute();
        String sessionResult = response.body().string();
        logger.info("会话数据:{}",sessionResult);
        int code = Integer.parseInt(JsonUtil.getString(sessionResult, "code"));

        // 数据递增
        currentDay = currentDay.plusDays(1);

        if (code == 200) {
            logger.info("sessionResult:" + sessionResult);
            String message = JsonUtil.getString(sessionResult, "message");
            // 重试3次
            String fileUrl = QiYuCommonUtils.getDownloadFileURL(this.check_file_url, message, this.qiyu_encrypt_key, this.app_key, okHttpClient, 3);
            logger.info("fileUrl:" + fileUrl);
            // 下载文件
            final String fileName = "ods_qiyu_user_session_user_message_d_" + DateFormatUtils.format(date, "yyyyMMdd");
            final String fileDir = this.fetch_data_path + "/ods_qiyu_user_session_user_message_d_" + DateFormatUtils.format(date, "yyyyMMdd");
            logger.info("解压的文件名为：[{}]", fileName);
            logger.info("解压的文件路径为：[{}]", fileDir);
            QiYuCommonUtils.downloadFile(fileUrl, this.fetch_data_path, fileName);
            logger.info("download file success...");
            String target_file = this.fetch_data_path + "/" + dt + "/";
            // 解压
            QiYuCommonUtils.unzip(fileDir + ".zip", target_file, unzip_pw);
            //上传
            final String messageFile = target_file + "/139671/message.txt";
            logger.info("messageFile: [{}]", messageFile);
            String ods_qiyu_user_message_d_key = Constants.HW_OBS_BASE_QIYU_OBJECT_key + "/ods_qiyu_user_message_d/dt=" + dt + "/old_message_" + dt + ".txt";
            logger.info("清洗七鱼会话MESSAGE消息数据导入上传的地址为：[{}]", ods_qiyu_user_message_d_key);
            this.obsClient.putObject(Constants.HW_OBS_BASE_BUCKET_NAME, ods_qiyu_user_message_d_key, new File(messageFile));

            final String sessionFile = target_file + "/139671/session.txt";
            logger.info("sessionFile: [{}]", sessionFile);
            String ods_qiyu_user_session_d = Constants.HW_OBS_BASE_QIYU_OBJECT_key + "/ods_qiyu_user_session_d/dt=" + dt + "/old_session_" + dt + ".txt";
            logger.info("清洗七鱼会话SESSION数据导入上传的地址为：[{}]", ods_qiyu_user_session_d);
            this.obsClient.putObject(Constants.HW_OBS_BASE_BUCKET_NAME, ods_qiyu_user_session_d, new File(sessionFile));

            /**
             * 删除本地文件
             */
            this.deleteFile(new File(target_file));

        } else {
            logger.info("执行报错了：[{}]", sessionResult);
        }
    }
}
