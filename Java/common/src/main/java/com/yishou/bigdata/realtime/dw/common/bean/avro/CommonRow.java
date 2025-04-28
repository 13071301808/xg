package com.yishou.bigdata.realtime.dw.common.bean.avro;

import com.alibaba.fastjson.JSON;

import java.util.Map;

public class CommonRow {

    protected String primaryKey;
    protected String db;
    protected String tb;
    protected Map<String, Column> columns;
    /**
     * INSERT = 0;
     * DEL = 1;
     * UPDATE = 2;
     * ALTER = 3;
     */
    protected int event = -1;
    protected String sql;

    protected long createTime = System.currentTimeMillis();

    protected long nanoTime = System.nanoTime();

    protected long sendTime;

    public long getNanoTime() {
        return nanoTime;
    }

    public void setNanoTime(long nanoTime) {
        this.nanoTime = nanoTime;
    }

    public CommonRow() {
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public CommonRow(String primaryKey, String db, String tb, Map<String, Column> columns, int event, String sql) {
        this.primaryKey = primaryKey;
        this.db = db;
        this.tb = tb;
        this.columns = columns;
        this.event = event;
        this.sql = sql;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTb() {
        return tb;
    }

    public void setTb(String tb) {
        this.tb = tb;
    }

    public Map<String, Column> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Column> columns) {
        this.columns = columns;
    }

    public int getEvent() {
        return event;
    }

    public void setEvent(int event) {
        this.event = event;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String fetchKey() {
        final StringBuilder sb = new StringBuilder();
        sb.append(db);
        sb.append(".");
        sb.append(tb);
        return sb.toString();
    }

    @Override
    public String toString() {
        return "CommonRow{" +
                "primaryKey='" + primaryKey + '\'' +
                ", db='" + db + '\'' +
                ", tb='" + tb + '\'' +
                ", colums=" + JSON.toJSONString(columns) +
                ", event=" + event +
                ", sql='" + sql + '\'' +
                '}';
    }

}
