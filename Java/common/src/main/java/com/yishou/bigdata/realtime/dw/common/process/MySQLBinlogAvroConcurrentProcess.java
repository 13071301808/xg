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
public class MySQLBinlogAvroConcurrentProcess extends ProcessFunction<CommonRow, CommonRow> {

    private String[] tbs ;

    public MySQLBinlogAvroConcurrentProcess(String info) {
        this.tbs = info.split(",");
    }

    static Logger logger = LoggerFactory.getLogger(MySQLBinlogAvroConcurrentProcess.class);

    @Override
    public void processElement(CommonRow value, Context ctx, Collector<CommonRow> out) throws Exception {

        try {
            for (String tball : tbs) {
                final String[] split = tball.split("\\.");
                // 当传入数据为null，即说明反序列化失败
                if (value != null && split[0].equals(value.getDb()) && split[1].equals(value.getTb())) {
                    out.collect(value);
                    return;
                }
            }
        } catch (Exception e) {
            logger.warn(
                    "***** MySQLBinlogAvro主题解析异常，抛出的异常信息为：{}",
                    e.getMessage()
            );
        }

    }

}
