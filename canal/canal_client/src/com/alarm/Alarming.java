package com.alarm;

import java.util.*;

/**
 * Created by WangXiao on 2018/8/16.
 * 异常告警  错略限制性发送
 */
public class Alarming {
    //错误类型 时间戳
    private Map<String, Long> error_cache = new HashMap<>();
    private static Alarming instance = null;
    private final int MAX_ERROR_COUNT = 1000;
    private int current_error_num = 0;

    private final long SEND_SPAN = 2 * 60 * 60 * 1000L;
    private final long CLEAR_OLD_SPAN = 24 * 60 * 60 * 1000L;

    public static Alarming getInstance(){
        if (instance == null) {
            synchronized (Alarming.class){
                if (instance == null) {
                    instance = new Alarming();
                }
            }
        }
        return instance;
    }

    /**
     * 发送异常消息给钉钉群
     * 出现异常时条数太多，粗略限制一下 按消息内容和时间戳来限制
     * @param errorInfo 异常消息内容。 格式要求： 来源##错误消息  ,不能带时间信息
     */
    public void add(String errorInfo){
        try {
            long nowTime = new Date().getTime();
            Long lastModifiedTime = error_cache.get(errorInfo);
            if (null == lastModifiedTime || (nowTime - lastModifiedTime) >= SEND_SPAN) {
                error_cache.put(errorInfo, nowTime);
                Dingding.alarm("您有新的异常：" + errorInfo + " ,请及时处理!");
            }
            //清理一下 防止随时间推移越积越多发生OOM
            current_error_num++;
            if (current_error_num >= MAX_ERROR_COUNT) {
                synchronized (Alarming.this) {
                    current_error_num = 0;
                    error_cache.clear();
                }
            }
        }catch (Exception e){
            System.out.println("清理异常..." + e.getMessage());
            current_error_num = MAX_ERROR_COUNT;
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        Alarming.getInstance().add("sls-hbase##999牌感冒灵");
        Alarming.getInstance().add("sls-hbase##999牌感冒灵");
    }
}
