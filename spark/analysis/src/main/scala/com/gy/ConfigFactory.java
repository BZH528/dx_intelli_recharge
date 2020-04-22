package com.gy;

/**
 * Created by Administrator on 2020/4/8 0008.
 */
public class ConfigFactory {
    public static String kafkaipport="10.193.11.13:9092,10.193.11.14:9092";
    public static String kafkazookeeper="10.193.11.11:2181,10.193.11.12:2181,10.193.11.13:2181";
    public static String kafkatopic="dyg_burydata";
    public static String kafkagroupid="two";
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

    public static String sparkstreamname="burydataAnalysis";
    public static int sparkstreamseconds=30;
    public static String checkpointdir="hdfs:///user/spark_works/checkdir/";
    public static String confPath="/opt/goldenBoxConf/";
}
