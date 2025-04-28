package com.yishou.bigdata.realtime.dw.common.bean.avro;

public class Column {

    private String name;
    private boolean isKey;
    private String mysqlType;
    private boolean isUpdate;
    private boolean null_val;
    private String value;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setKey(boolean key) {
        isKey = key;
    }

    public String getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(String mysqlType) {
        this.mysqlType = mysqlType;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public void setUpdate(boolean update) {
        isUpdate = update;
    }

    public boolean isNull_val() {
        return null_val;
    }

    public void setNull_val(boolean null_val) {
        this.null_val = null_val;
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", isKey=" + isKey +
                ", mysqlType='" + mysqlType + '\'' +
                ", isUpdate=" + isUpdate +
                ", null_val=" + null_val +
                ", value='" + value + '\'' +
                '}';
    }

}
