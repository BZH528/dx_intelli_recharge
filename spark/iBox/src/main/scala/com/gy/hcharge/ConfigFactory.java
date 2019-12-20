package com.gy.hcharge;

import com.google.common.io.Resources;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;


/**
 * Created by Administrator on 2019/11/14 0014.
 */
public class ConfigFactory {

    public static String kafkaipport="10.193.11.13:9092,10.193.11.14:9092";
    public static String kafkazookeeper="10.193.11.11:2181,10.193.11.12:2181,10.193.11.13:2181";
    public static String[] kafkatopic="buryData,order".split(",");
    public static String kafkagroupid="one";
    public static String mysqlurl="jdbc:mysql://10.193.11.12:3306";
        public static String mysqlusername="finebi";
    public static String mysqlpassword="Fine>1939BI";
    public static String database="xiaojinhe";
    public static String realTable="realtime_feature";
    public static String dayTable="daytime_feature";

    public static String today_visit_user="tmp:todayVisitUser";
    public static String all_user="tmp:allUser";
    public static String family="non";
    public static String column="chanal";
    public static String hbasezookeeper="10.193.11.11:2181,10.193.11.12:2181,10.193.11.13:2181";

    public static String sparkstreamname="GlodenBox";
    public static int sparkstreamseconds=20;
    public static String checkpointdir="hdfs:///user/spark_works/checkdir/";
    public static String confPath="/opt/goldenBoxConf/";

    /**
     * 初始化所有的通用信息
     */
     //static{readCommons();}

    /**
     * 读取commons.xml文件
     */
    private static void readCommons(){
        SAXReader reader = new SAXReader(); // 构建xml解析器
        Document document = null;
        try{
            document = reader.read(new File("/opt/goldenBoxConf/commons.xml"));
        }catch (DocumentException e){
            e.printStackTrace();
        }

        if(document != null){
            Element root = document.getRootElement();

            Element kafkaElement = root.element("kafka");
            kafkaipport = kafkaElement.element("ipport").getText();
            kafkazookeeper = kafkaElement.element("zookeeper").getText();
            kafkatopic = kafkaElement.element("topic").getText().split(",");
            kafkagroupid = kafkaElement.element("groupid").getText();


            Element mysqlElement = root.element("mysql");
            mysqlurl = mysqlElement.element("url").getText();
            mysqlusername = mysqlElement.element("username").getText();
            mysqlpassword = mysqlElement.element("password").getText();
            realTable = mysqlElement.element("realTable").getText();
            dayTable = mysqlElement.element("dayTable").getText();
            database = mysqlElement.element("dataBase").getText();


            Element hbaseElement = root.element("hbase");
            today_visit_user = hbaseElement.element("todayVisitUser").getText();
            all_user = hbaseElement.element("allUser").getText();
            family = hbaseElement.element("family").getText();
            column =  hbaseElement.element("column").getText();
            hbasezookeeper =  hbaseElement.element("hbasezookeeper").getText();

            Element sparkElement = root.element("spark");
            sparkstreamname = sparkElement.element("streamname").getText();
            sparkstreamseconds = Integer.valueOf(sparkElement.element("seconds").getText());

            Element pathElement = root.element("path");
            checkpointdir = pathElement.element("checkpointdir").getText();
            confPath = pathElement.element("confdir").getText();
        }else {
            System.out.println("erro");
        }
    }

}
