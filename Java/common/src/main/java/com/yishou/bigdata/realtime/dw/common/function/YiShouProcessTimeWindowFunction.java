package com.yishou.bigdata.realtime.dw.common.function;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public abstract class YiShouProcessTimeWindowFunction<IN, OUT, KEY, W extends TimeWindow> extends ProcessWindowFunction<IN, OUT, KEY, W> {
    public ValueState<Long> windowEndTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.windowEndTime = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "window_end_time"
                , Types.LONG
        ));
        this.ysopen(parameters);
    }

    @Override
    public void process(KEY key, ProcessWindowFunction<IN, OUT, KEY, W>.Context context, Iterable<IN> elements, Collector<OUT> out) throws Exception {
        final Long windowEndTime = this.windowEndTime.value();
        if (windowEndTime == null) {
            this.windowEndTime.update(DateUtils.addDays(DateUtils.parseDate(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd 07:00:00"), "yyyy-MM-dd HH:mm:ss"), 1).getTime());
        } else {
            /**
             * 新的窗口
             */
            if (windowEndTime < context.window().getEnd()) {
                this.windowEndTime.update(DateUtils.addDays(DateUtils.parseDate(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd 07:00:00"), "yyyy-MM-dd HH:mm:ss"), 1).getTime());
                scheduler7am(key, context, elements, out);
            }
        }
        this.ysprocess(key, context, elements, out);
    }

    public abstract void ysprocess(KEY key, ProcessWindowFunction<IN, OUT, KEY, W>.Context context, Iterable<IN> elements, Collector<OUT> out) throws Exception;


    public abstract void scheduler7am(KEY key, ProcessWindowFunction<IN, OUT, KEY, W>.Context context, Iterable<IN> elements, Collector<OUT> out) throws Exception;

    public abstract void ysopen(Configuration parameters) throws Exception;
}
