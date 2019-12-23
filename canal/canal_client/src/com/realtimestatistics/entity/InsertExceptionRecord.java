package com.realtimestatistics.entity;

import java.util.Date;
import java.util.Map;

/**
 * Created by WangXiao on 2018/11/21.
 * 记录修改的字段信息 在原基础上附加插入Hbase失败时间
 */
public class InsertExceptionRecord {
    public Long failedTime = 0L;
    public String value = null;

    public InsertExceptionRecord(Long failedTime, String value){
        this.failedTime = failedTime;
        this.value = value;
    }

    public Long getFailedTime() {
        return failedTime;
    }

    public void setFailedTime(Long failedTime) {
        this.failedTime = failedTime;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String toString(){
        return "failedTime:" + failedTime + ";value=" + value;
    }

    public static void main(String[] args) {
        System.out.println(new Date().getTime());
    }
}
