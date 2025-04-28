package com.yishou.bigdata.realtime.dw.common.utils;

/**
 * @date: 2022/12/17
 * @author: yangshibiao
 * @desc: 对类操作的工具类
 */
public class ClassUtil {

    /**
     * 截取最后的类名
     *
     * @param clazzName 类全路径名
     * @return 需要的类名
     */
    public static String getClassName(String clazzName) {
        return clazzName.substring(clazzName.lastIndexOf('.') + 1);
    }

}
