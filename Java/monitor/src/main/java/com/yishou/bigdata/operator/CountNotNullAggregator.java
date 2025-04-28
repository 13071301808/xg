package com.yishou.bigdata.operator;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountNotNullAggregator implements AggregateFunction<JSONObject, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
    public static Logger logger = LoggerFactory.getLogger(CountNotNullAggregator.class);

    // 初始化累加器，记录 true 和 false 的数量
    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0, 0); // (trueCount, falseCount)
    }

    // 根据字段值更新累加器
    @Override
    public Tuple2<Integer, Integer> add(JSONObject value, Tuple2<Integer, Integer> accumulator) {
        String isNotNull = value.getString("is_not_null");
        if (isNotNull != null) {
            if ("true".equalsIgnoreCase(isNotNull)) {
                return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
            } else if ("false".equalsIgnoreCase(isNotNull)) {
                return Tuple2.of(accumulator.f0, accumulator.f1 + 1);
            }
        }
        return accumulator;
    }

    // 返回最终结果
    @Override
    public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> accumulator) {
        return accumulator;
    }

    // 合并多个累加器（仅在会话窗口等需要合并时使用）
    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}

