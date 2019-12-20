package com.sync.timer;

import com.realtimestatistics.CacheRecovery;
import com.realtimestatistics.entity.InsertExceptionRecord;
import com.sync.common.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by WangXiao on 2018/11/22.
 * 定时将之前插入hbase异常的记录重新插入hbase
 * 间隔执行：waitLoopTime 秒
 */
public class ReInsertHbase implements Runnable {
    private final Long waitLoopTime = 90L;
    public void reInsert(){
        //
        Map<String,Map<String,Map<String,InsertExceptionRecord>>> cacheRecords = CacheRecovery.getInstance().deepCopyAndClear();
        for (Map.Entry<String, Map<String, Map<String, InsertExceptionRecord>>> sm : cacheRecords.entrySet()) {
            String tableName = sm.getKey().split("-")[0];
            for (Map.Entry<String, Map<String, InsertExceptionRecord>> ele : sm.getValue().entrySet()) {
                String rowKey = ele.getKey();
                Map<String,Long> oneRecord = new HashMap<>();
                try {
                    Put put = new Put(Bytes.toBytes(rowKey));
                    HBaseConnection.getInstance().insertPut(tableName, put);
                    System.out.println("重试::插入成功.tablename=" + tableName + ",rowkey=" + rowKey);

                }catch (Exception e){
                    CacheRecovery.getInstance().add(sm.getKey(),rowKey,ele.getValue());
                    System.out.println("重试::插入异常!tablename=" + tableName + ",rowkey=" + rowKey + "  " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
    public static int compare_today(String date1) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String date2 = df.format(new Date());
        try {
            Date dt1 = df.parse(date1);
            Date dt2 = df.parse(date2);
            if (dt1.getTime() > dt2.getTime()) {
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }


    public void run(){
        // 程序启动时，读取上次的缓存信息
        CacheRecovery.getInstance().readFromLastTimeStop();
        while(true) {
            try {
                this.reInsert();
                System.out.println("等待下一次补录......");
                Thread.sleep(5*1000L);
                if (CacheRecovery.getInstance().getRecordsNum() > 1000){
                    continue;
                }
                Thread.sleep(this.waitLoopTime * 1000);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("重试插入发生一次异常");
            }
        }
    }
}
