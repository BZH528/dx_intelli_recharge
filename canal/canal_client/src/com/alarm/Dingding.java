package com.alarm;

import com.sync.common.GetProperties;
import com.sync.common.UrlHttpSender;
import org.apache.log4j.Logger;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by WangXiao on 2018/8/16.
 * 简单的钉钉告警实现
 */
public class Dingding {
    private final static Logger logger = Logger.getLogger(Dingding.class);
    //private static String dingding_url = "https://oapi.dingtalk.com/robot/send?access_token=4476688e782c245737240bf01b75a35f1e560bb315b6166bc2ffcfcb6efbf1fc";
    private static String dingding_url = "http://dingding.19ego.cn/WarnInfo/inside";
    private static String channel = "big_data";
    private static String telephone = "15652230847";
    static{
        String config_token = GetProperties.hbase_table_pk.getProperty("dingding");
        String config_channel = GetProperties.hbase_table_pk.getProperty("dingding_channel");
        String config_telephone =  GetProperties.hbase_table_pk.getProperty("dingding_telephone");
        if (null != config_token && !"".equals(config_token)){
            dingding_url = config_token;
        }
        if (null != config_channel && !"".equals(config_channel)){
            channel = config_channel;
        }
        if (null != config_telephone && !"".equals(config_telephone)){
            telephone = config_telephone;
        }

    }

    public static boolean alarm(String message){
        Map<String, String> header = new HashMap<>();
        header.put("Content-Type","application/json; charset=utf-8");
        String jsonparam = "{\"chan\":\"dingding_channel\",\"msg\":[{\"msg\":\"messageinfo\",\"sendto\":\"dingding_telephone\",\"level\":\"3\"}]}";
        try {
            jsonparam = jsonparam.replace("dingding_channel",channel);
            jsonparam = jsonparam.replace("dingding_telephone",telephone);
            jsonparam = jsonparam.replace("messageinfo", URLEncoder.encode(message,"utf-8"));
            String result = UrlHttpSender.sendPost(dingding_url, jsonparam, header);
            logger.info("钉钉告警发送结果:" + result);
            System.out.println("jsonparam:" + jsonparam);
            System.out.println("钉钉告警发送结果:" + result);
        }catch (Exception e){e.printStackTrace();}
        return false;
    }

    public static void main(String[] args) {
        Dingding.alarm("bigdata，你好哦。");
    }
}
