package com.sync.process;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.realtimestatistics.CacheRecovery;
import com.sync.common.GetProperties;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by WangXiao on 2018/11/6.
 */
public class test {
    private final static Logger logger = Logger.getLogger(test.class);
    // 日志服务域名，根据实际情况填写
    private static String sEndpoint = "cn-beijing.log.aliyuncs.com";
    // 日志服务项目名称，根据实际情况填写
    private static String sProject = "canal-db1";
    // 日志库名称，根据实际情况填写
    private static String sLogstore = "tempstore";
    // 消费组名称，根据实际情况填写
    private static String sConsumerGroup = "consumerGroupX";
    // 消费数据的ak，根据实际情况填写
    private static String sAccessKeyId = "LTAIjJUDboX5yYG4";
    private static String sAccessKey = "5kss70hylRSdYSI9x2HL9NhvGgMaQq";

    private static test instance = null;
    private Map<String,String> inner = null;
    private int total = 0;

    public static test getInstance(){
        if (instance == null) {
            synchronized (CacheRecovery.class){
                if (instance == null) {
                    instance = new test();
                }
            }
        }
        return instance;
    }

    public test(){
        if (null == inner){
            inner = new HashMap<>();
        }
    }
    public void add(String key, String value){
            inner.put(key, value);
    }
    public void clear(){
        synchronized(test.class) {
            total = total + inner.size();
            inner.clear();
        }
    }

    public void process(){
        synchronized (test.class){}
    }

    public void print(){
        System.out.println("总数: " + total);
    }

    /*
 * 将时间转换为时间戳
 */
    public static long dateToStamp(String s){
        long res = 0L;
        try {
            SimpleDateFormat simpleDateFormat = null;
            if (s.indexOf(".") < 0) {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }else{
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            }
            Date date = simpleDateFormat.parse(s);
            long ts = date.getTime();
            res = Long.valueOf(String.valueOf(ts));
        }catch (Exception e){
            e.printStackTrace();
        }
        return res;
    }
    private static boolean deviationJudge(String hourTime){
        boolean result = true;
        SimpleDateFormat temp = new SimpleDateFormat("yyyy-MM-dd HH");
        try{
            if (new Date().getTime() - temp.parse(hourTime).getTime() > 8*3600*1000L){
                result = false;
            }
        }catch (Exception e){e.printStackTrace();}
        return result;
    }

    public static void main(String []args) throws Exception
    {
        Map<String,String> tempKV = new HashMap<>();
        tempKV.put("cd_coupon_data_id","100000");
        tempKV.put("cd_coupon_id","222222222222");
        tempKV.put("aaa","---------");

        String real_tableName = "csc_coupon_data";
        String pk = GetProperties.hbase_table_pk.getProperty(real_tableName + "_primarykey");
        if (null == pk || "".equals(pk)){
            System.out.println("表:" + real_tableName + " 未配置主键");
        }
        String[] ps_array = pk.split(",");
        System.out.println("长度：" + ps_array.length);
        String rowkey = "";
        for (int t_index = 0; t_index<ps_array.length; t_index++){
            System.out.println("键:" + ps_array[t_index]);
            String one_key = tempKV.get(ps_array[t_index]);
            System.out.println("值:" + one_key);
            if (null==one_key || "".equals(one_key)){
                rowkey = "";
                System.out.println("表:" + real_tableName + " 主键配置错误:" + ps_array[t_index]);
                break;
            }
            rowkey = rowkey + one_key + "_";
        }
        System.out.println("主键是:" + rowkey);
        if ("".equals(rowkey)){
            System.out.println("空的");
        }else{
            rowkey = rowkey.substring(0,rowkey.length()-1);
        }

        System.out.println("主键是:" + rowkey);

//        System.out.println(test.deviationJudge("2018-12-12 09"));
//
//        Thread one = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                int i = 0;
//                for (i = 0; i < 100000; i++){
//                    test.getInstance().add("k_"+i,"ppppppp");
//                    //System.out.println("插入成功");
//                }
//                //System.out.println("插入完成.i=" + i);
//            }
//        });
//        Thread oneone = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                int i = 0;
//                for (i = 0; i < 100000; i++){
//                    test.getInstance().add("p_"+i,"ppppppp");
//                    //System.out.println("插入成功");
//                }
//                //System.out.println("插入完成.i=" + i);
//            }
//        });
//
//        Thread two = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                int count = 150;
//                while(count > 0) {
//                    try{
//                        Thread.sleep(1);
//                    }catch (Exception e){}
//                    test.getInstance().clear();
//                    count--;
//                }
//            }
//        });
//
//        one.start();
//        oneone.start();
//        two.start();
//        try{
//            Thread.sleep(5000);
//        }catch (Exception e){}
//        test.getInstance().print();


//        Thread third = new Thread(new Runnable() {
//            @Override
//            public void run() {
//
//            }
//        })


//        Map<String, Object> map = new HashMap<>();
//        map.put("11", 11);
//        map.put("22", "22");
//        map.put("33", 33L);
//        map.put("44", 44.44);
//        Map<String,String> kv = new HashMap<>();
//        kv.put("_event_","ffff");
//        CacheRecovery.getInstance().add(0,"ssss","111111111111",kv);
//        System.out.println("Map: " + CacheRecovery.getInstance().getInnerMapSize());


//        // 第二个参数是消费者名称，同一个消费组下面的消费者名称必须不同，可以使用相同的消费组名称，不同的消费者名称在多台机器上启动多个进程，来均衡消费一个Logstore，这个时候消费者名称可以使用机器ip来区分。第9个参数（maxFetchLogGroupSize）是每次从服务端获取的LogGroup数目，使用默认值即可，如有调整请注意取值范围(0,1000]
//        LogHubConfig config = new LogHubConfig(sConsumerGroup, "consumer_1", sEndpoint, sProject, sLogstore, sAccessKeyId, sAccessKey, LogHubConfig.ConsumePosition.BEGIN_CURSOR);
//        ClientWorker worker = new ClientWorker(new SLS2EMRHbaseFactory(), config);
//        Thread thread = new Thread(worker);
//        //Thread运行之后，Client Worker会自动运行，ClientWorker扩展了Runnable接口。
//        System.out.println("start......");
//        thread.start();
//        System.out.println("end.....");
////        Thread.sleep(60 * 60 * 1000);
////        //调用worker的Shutdown函数，退出消费实例，关联的线程也会自动停止。
////        worker.shutdown();
////        //ClientWorker运行过程中会生成多个异步的Task，Shutdown之后最好等待还在执行的Task安全退出，建议sleep 30s。
////        Thread.sleep(30 * 1000);
    }
}
