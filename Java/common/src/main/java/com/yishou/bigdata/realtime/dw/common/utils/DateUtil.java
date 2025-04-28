package com.yishou.bigdata.realtime.dw.common.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @date: 2023/5/5
 * @author: yangshibiao
 * @desc: 时间工具类
 */
public class DateUtil {

    /**
     * 专场日日期格式解析对象（yyyyMMdd）
     */
    private static final FastDateFormat DF_SPECIAL_DATE = FastDateFormat.getInstance("yyyyMMdd");

    /**
     * 普通日期格式解析对象（yyyyMMdd）
     */
    private static final FastDateFormat DF_DATE = FastDateFormat.getInstance("yyyyMMdd");

    /**
     * 普通日期格式解析对象（yyyy-MM-dd HH:mm:ss）
     */
    private static final FastDateFormat DF_DATE_COMPLEX = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    /**
     * 将毫秒值（13位时间戳）转换成专场日（yyyyMMdd）
     * 注意：需要减7小时（传入的数据不用减，在此工具类中已进行处理）
     *
     * @param millisecond 毫秒值（13位时间戳）
     * @return 专场日（yyyyMMdd）
     */
    public static String millisecondToSpecialDate(long millisecond) {
        return DF_SPECIAL_DATE.format(millisecond - 7 * 60 * 60 * 1000);
    }

    /**
     * 将秒值（10位时间戳）转换成专场日（yyyyMMdd）
     * 注意：需要减7小时（传入的数据不用减，在此工具类中已进行处理）
     *
     * @param second 秒值（10位时间戳）
     * @return 专场日（yyyyMMdd）
     */
    public static String secondToSpecialDate(long second) {
        return DF_SPECIAL_DATE.format(second * 1000L - 7 * 60 * 60 * 1000);
    }

    /**
     * 将毫秒值（13位时间戳）转换成普通日期格式（yyyyMMdd）
     *
     * @param millisecond 毫秒值（13位时间戳）
     * @return 普通日期（yyyyMMdd）
     */
    public static String millisecondToDate(long millisecond) {
        return DF_DATE.format(millisecond);
    }

    /**
     * 将秒值（10位时间戳）转换成普通日期格式（yyyyMMdd）
     *
     * @param second 秒值（10位时间戳）
     * @return 普通日期（yyyyMMdd）
     */
    public static String secondToDate(long second) {
        return DF_DATE.format(second * 1000L);
    }

    /**
     * 将毫秒值（13位时间戳）转换成普通日期格式（yyyy-MM-dd HH:mm:ss）
     *
     * @param millisecond 毫秒值（13位时间戳）
     * @return 普通日期（yyyy-MM-dd HH:mm:ss）
     */
    public static String millisecondToDateComplex(long millisecond) {
        return DF_DATE_COMPLEX.format(millisecond);
    }

    /**
     * 将秒值（10位时间戳）转换成普通日期格式（yyyy-MM-dd HH:mm:ss）
     *
     * @param second 秒值（10位时间戳）
     * @return 普通日期（yyyy-MM-dd HH:mm:ss）
     */
    public static String secondToDateComplex(long second) {
        return DF_DATE_COMPLEX.format(second * 1000L);
    }

    /**
     * 将传入的字符串日期转换为时间毫秒数（东八区）
     *
     * @param dateStr   字符串日期
     * @param formatter 该日期对应的时间格式
     * @return 毫秒数
     */
    public static Long toTimestampForDateStr(String dateStr, String formatter) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern(formatter));
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

}
