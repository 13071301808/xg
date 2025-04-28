package com.yishou.bigdata.realtime.dw.common.process;

import com.yishou.bigdata.realtime.dw.common.bean.avro.CommonRow;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date: 2023/2/21
 * @author: yangshibiao
 * @desc: MySQLBinlogAvro主题解析
 */
public class MySQLBinlogAvroProcess extends ProcessFunction<CommonRow, CommonRow> {

    private String db;
    private String tb;

    public MySQLBinlogAvroProcess(String info) {
        String[] infos = info.split("\\.");
        this.db = infos[0];
        this.tb = infos[1];
    }

    static Logger logger = LoggerFactory.getLogger(MySQLBinlogAvroProcess.class);

    @Override
    public void processElement(CommonRow value, Context ctx, Collector<CommonRow> out) throws Exception {

        try {
            // 当传入数据为null，即说明反序列化失败
            if (value != null && db.equals(value.getDb()) && tb.equals(value.getTb())) {
                out.collect(value);
            }
        } catch (Exception e) {
            logger.warn(
                    "***** MySQLBinlogAvro主题解析异常，抛出的异常信息为：{}",
                    e.getMessage()
            );
        }

    }

}
