package com.realtimestatistics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.realtimestatistics.entity.InsertExceptionRecord;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by WangXiao on 2018/11/21.
 * 记录插入Hbase异常的数据，后台进行重试插入
 * 插入hbase失败的原因是:hbase的region分裂时会插入异常
 */
public class CacheRecovery {
    //<tablename-shardid, <rowkey , <fieldname, InsertExceptionRecord(changetime,value)>>
    private static Map<String,Map<String,Map<String,InsertExceptionRecord>>> cacheRecords = null;
    private static Map<Integer,Map<String,CopyOnWriteArrayList<Put>>> referBatchBlock = null;
    private static CacheRecovery instance = null;

    public static CacheRecovery getInstance(){
        if (instance == null) {
            synchronized (CacheRecovery.class){
                if (instance == null) {
                    instance = new CacheRecovery();
                }
            }
        }
        return instance;
    }

    public CacheRecovery(){
        if(null == cacheRecords){
            cacheRecords = new HashMap<>();
        }
        if (null == referBatchBlock){
            referBatchBlock = new HashMap<>();
        }
    }

    public int getRecordsNum(){
        int num = 0;
        if(null != cacheRecords){
            num = cacheRecords.size();
        }
        return num;
    }

    /**
     * 引用SLS消费客户端的记录块，保证在关闭程序时这些块记录不被丢失，下次启动程序时自动恢复
     * @param ShardId logstore的分区ID
     * @param oneBlock 准备写入Hbase的内存块
     */
    public static void referBatchBlcok(int ShardId, Map<String,CopyOnWriteArrayList<Put>> oneBlock){
        referBatchBlock.put(ShardId,oneBlock);
    }
    //获取某个分区的失败插入记录
    //应该是不需要加锁，key是'talbename_shardid',每个shardid只会出现一次
    public static Map<String,Map<String,InsertExceptionRecord>> getValue(String tableName_ShardId){
        Map<String,Map<String,InsertExceptionRecord>> result = cacheRecords.get(tableName_ShardId);
        if(null == result){
            synchronized (CacheRecovery.class) {
                result = new HashMap<>();
                cacheRecords.put(tableName_ShardId, result);
            }
        }
        return result;
    }

    //添加插入异常记录
    public static void add(int shardId, String hbaseTableName,String rowkey,Map<String,String> kv){
        //记录失败插入时间作为记录的时间戳 用于后期插入与hbase中已有记录各个字段比较时间戳
        long failedTimeStamp = dateToStamp(hbaseTableName + ":" + rowkey, kv.get("gmt_modified"));

        Map<String,Map<String,InsertExceptionRecord>> onlyUpdates = CacheRecovery.getValue(hbaseTableName + "-" + shardId);
        String type = kv.get("_event_");

        if (type.equals("row_update")){
            //只要变化的记录
            for (Map.Entry<String,String> ele : kv.entrySet()){
                String tempKey = ele.getKey();
                if (ele.getKey().indexOf("_") != 0){
                    if (!ele.getValue().equals(kv.get("_old_" + tempKey))){
                        Map<String,InsertExceptionRecord> onepart = onlyUpdates.get(rowkey);
                        if (onepart == null){
                            onepart = new HashMap<>();
                            onlyUpdates.put(rowkey,onepart);
                        }
                        InsertExceptionRecord oneFailedRecord = onepart.get(tempKey);
                        if (null == oneFailedRecord) {
                            oneFailedRecord = new InsertExceptionRecord(failedTimeStamp, ele.getValue());
                            onepart.put(tempKey, oneFailedRecord);
                        }else{
                            oneFailedRecord.setFailedTime(failedTimeStamp);
                            oneFailedRecord.setValue(ele.getValue());
                        }
                    }
                }
            }
        }else if(type.equals("row_insert")){
            //排除系统字段
            for (Map.Entry<String,String> ele : kv.entrySet()){
                String tempKey = ele.getKey();
                if (ele.getKey().indexOf("_") != 0){
                    Map<String,InsertExceptionRecord> onepart = onlyUpdates.get(rowkey);
                    if (onepart == null){
                        onepart = new HashMap<>();
                        onlyUpdates.put(rowkey,onepart);
                    }
                    InsertExceptionRecord oneFailedRecord = new InsertExceptionRecord(failedTimeStamp,ele.getValue());
                    onepart.put(tempKey,oneFailedRecord);
                }
            }
        }else{
            //do nothing
        }
    }
    //后补插入异常时使用这个追加到自身缓存（补充插入都是单Put插入）
    public static void add(String region,String rowkey,Map<String,InsertExceptionRecord> kv){
        synchronized (CacheRecovery.class) {
            Map<String, Map<String, InsertExceptionRecord>> onlyUpdates = CacheRecovery.getValue(region);
            Map<String, InsertExceptionRecord> onepart = onlyUpdates.get(rowkey);
            if (onepart == null) {
                onlyUpdates.put(rowkey, kv);
            } else {
                for (Map.Entry<String, InsertExceptionRecord> ele : kv.entrySet()) {
                    if (null == onepart.get(ele.getKey())
                            || onepart.get(ele.getKey()).getFailedTime() < ele.getValue().getFailedTime()) {
                        onepart.put(ele.getKey(), ele.getValue());
                    }
                }
            }
        }
    }
    //首次批量插入异常时使用这个追加到自身缓存 (批量补充插入异常时，后补插入使用单Put插入)
    public static void add(int shardId, String hbaseTableName, List<Put> puts){
        //记录失败插入时间作为记录的时间戳 用于后期插入与hbase中已有记录各个字段比较时间戳 20181130删除 使用gmt_modified字段值作为时间戳
        //long failedTimeStamp = System.currentTimeMillis();
        synchronized (CacheRecovery.class) {
            Map<String, Map<String, InsertExceptionRecord>> onlyUpdates = CacheRecovery.getValue(hbaseTableName + "-" + shardId);
            for (Put onePut : puts) {
                String rowkey = new String(onePut.getRow());
                Map<String, InsertExceptionRecord> onepart = onlyUpdates.get(rowkey);
                if (onepart == null) {
                    onepart = new HashMap<>();
                    onlyUpdates.put(rowkey, onepart);
                }
                // TODO: 2018/11/28  目前只支持一个列簇
                NavigableMap<byte[], List<Cell>> temp = onePut.getFamilyCellMap();
                for (Map.Entry<byte[], List<Cell>> ele : temp.entrySet()) {
                    for (Cell cell : ele.getValue()) {
                        InsertExceptionRecord insertExceptionRecord = new InsertExceptionRecord(cell.getTimestamp(), new String(CellUtil.cloneValue(cell)));
                        onepart.put(new String(CellUtil.cloneQualifier(cell)), insertExceptionRecord);
                    }
                }
            }
        }
    }


    public static void delOneEle(String region, String deleteKey){
        Map<String,Map<String,InsertExceptionRecord>> oneRecord = cacheRecords.get(region);
        if(null != oneRecord){
            oneRecord.remove(deleteKey);
        }
    }
    public static Map<String,Map<String,Map<String,InsertExceptionRecord>>> deepCopy(){
        Map<String, Map<String, Map<String, InsertExceptionRecord>>> result = new HashMap<>();
        synchronized (CacheRecovery.class) {
            for (Map.Entry<String, Map<String, Map<String, InsertExceptionRecord>>> sm : cacheRecords.entrySet()) {
                Map<String, Map<String, InsertExceptionRecord>> temp1 = new HashMap<>();
                result.put(sm.getKey(), temp1);
                for (Map.Entry<String, Map<String, InsertExceptionRecord>> ele : sm.getValue().entrySet()) {
                    Map<String, InsertExceptionRecord> temp2 = new HashMap<>();
                    temp1.put(ele.getKey(), temp2);
                    for (Map.Entry<String, InsertExceptionRecord> one : ele.getValue().entrySet()) {
                        InsertExceptionRecord temp3 = new InsertExceptionRecord(one.getValue().getFailedTime(), one.getValue().getValue());
                        temp2.put(one.getKey(), temp3);
                    }
                }
            }
        }
        return result;
    }
    public static Map<String,Map<String,Map<String,InsertExceptionRecord>>> deepCopyAndClear(){
        Map<String, Map<String, Map<String, InsertExceptionRecord>>> result = new HashMap<>();
        synchronized (CacheRecovery.class) {
            for (Map.Entry<String, Map<String, Map<String, InsertExceptionRecord>>> sm : cacheRecords.entrySet()) {
                Map<String, Map<String, InsertExceptionRecord>> temp1 = new HashMap<>();
                result.put(sm.getKey(), temp1);
                for (Map.Entry<String, Map<String, InsertExceptionRecord>> ele : sm.getValue().entrySet()) {
                    Map<String, InsertExceptionRecord> temp2 = new HashMap<>();
                    temp1.put(ele.getKey(), temp2);
                    for (Map.Entry<String, InsertExceptionRecord> one : ele.getValue().entrySet()) {
                        InsertExceptionRecord temp3 = new InsertExceptionRecord(one.getValue().getFailedTime(), one.getValue().getValue());
                        temp2.put(one.getKey(), temp3);
                    }
                }
            }
            //清除cacheRecords中元素
            for (Iterator<Map.Entry<String,Map<String,Map<String,InsertExceptionRecord>>>> it = cacheRecords.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, Map<String, Map<String, InsertExceptionRecord>>> item = it.next();
                for (Iterator<Map.Entry<String, Map<String, InsertExceptionRecord>>> iitt = item.getValue().entrySet().iterator(); iitt.hasNext(); ) {
                    Map.Entry<String, Map<String, InsertExceptionRecord>> tt = iitt.next();
                    tt.getValue().clear();
                }
                item.getValue().clear();
            }
        }
        return result;
    }

    public static void clear(){
        synchronized (CacheRecovery.class) {
            for (Iterator<Map.Entry<String,Map<String,Map<String,InsertExceptionRecord>>>> it = cacheRecords.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, Map<String, Map<String, InsertExceptionRecord>>> item = it.next();
                for (Iterator<Map.Entry<String, Map<String, InsertExceptionRecord>>> iitt = item.getValue().entrySet().iterator(); iitt.hasNext(); ) {
                    Map.Entry<String, Map<String, InsertExceptionRecord>> tt = iitt.next();
                    tt.getValue().clear();
                }
                item.getValue().clear();
            }
        }
    }

    public static ArrayList<String> getKeys(){
        ArrayList<String> results = new ArrayList<>();
        for (Map.Entry<String,Map<String,Map<String,InsertExceptionRecord>>> sm : cacheRecords.entrySet()){
            results.add(sm.getKey());
        }
        return results;
    }
    public static Map<String,Map<String,InsertExceptionRecord>> getOne(String oneKey){
        return cacheRecords.get(oneKey);
    }

    public static void show(Map<String,Map<String,Map<String,InsertExceptionRecord>>> hs){
        for (Map.Entry<String,Map<String,Map<String,InsertExceptionRecord>>> sm : hs.entrySet()){
            System.out.println(sm.getKey());
            for (Map.Entry<String,Map<String,InsertExceptionRecord>> ele : sm.getValue().entrySet()){
                System.out.println(ele.getKey() + " :: ");
                for (Map.Entry<String,InsertExceptionRecord> one : ele.getValue().entrySet()){
                    System.out.println(one.getKey() + " : " + one.getValue().toString());
                }
            }
        }
    }
    public static void show(){
        for (Map.Entry<String,Map<String,Map<String,InsertExceptionRecord>>> sm : cacheRecords.entrySet()){
            System.out.println(sm.getKey());
            for (Map.Entry<String,Map<String,InsertExceptionRecord>> ele : sm.getValue().entrySet()){
                System.out.println(ele.getKey() + " :: ");
                for (Map.Entry<String,InsertExceptionRecord> one : ele.getValue().entrySet()){
                    System.out.println(one.getKey() + " : " + one.getValue().toString());
                }
            }
        }
    }
    //读取本地缓存文件到内存中
    //  './tmp.cacheRecovery': 上次结束进程时的缓存内容文件 json格式
    public static void readFromLastTimeStop(){
        synchronized (CacheRecovery.class) {
            JSONObject jsonObject = null;
            try {
                File file = new File("./tmp.cacheRecovery");
                if (!(file.exists() && file.isFile())) {
                    return;
                }
                String content = FileUtils.readFileToString(file, "UTF-8");
                jsonObject = JSON.parseObject(content);
                file.delete();
            } catch (Exception e) {
                System.out.println("启动时读取缓存文件异常." + e.getMessage());
                e.printStackTrace();
            }
            for (JSONObject.Entry<String, Object> first : jsonObject.entrySet()) {
                Map<String, Map<String, InsertExceptionRecord>> temp1 = CacheRecovery.getInstance().getValue(first.getKey());
                JSONObject tempJSONObject = (JSONObject) first.getValue();
                for (JSONObject.Entry<String, Object> second : tempJSONObject.entrySet()) {
                    Map<String, InsertExceptionRecord> ff = new HashMap<>();
                    temp1.put(second.getKey(), ff);
                    JSONObject tempJSONObject2 = (JSONObject) second.getValue();
                    for (JSONObject.Entry<String, Object> third : tempJSONObject2.entrySet()) {
                        JSONObject tempJSONObject3 = (JSONObject) third.getValue();
                        InsertExceptionRecord insertExceptionRecord = new InsertExceptionRecord(tempJSONObject3.getLong("failedTime"), tempJSONObject3.getString("value"));
                        ff.put(third.getKey(), insertExceptionRecord);
                    }
                }
            }
        }
    }
    //进程结束时，保存内存内容到本地文件./tmp.cacheRecovery  json格式
    public static void save(){
        synchronized (CacheRecovery.class) {
            //补充各个分片中尚未写入hbase的记录
            for (Map.Entry<Integer,Map<String,CopyOnWriteArrayList<Put>>> oneShard : referBatchBlock.entrySet()){
                for (Map.Entry<String,CopyOnWriteArrayList<Put>> oneBlock : oneShard.getValue().entrySet()) {
                    add(oneShard.getKey(), oneBlock.getKey(), oneBlock.getValue());
                }
            }
            //将缓存中数据存储到本地文件中 便于下次恢复
            JSONObject itemJSONObj = JSONObject.parseObject(JSON.toJSONString(cacheRecords));
            System.out.println(itemJSONObj.toString());
            try {
                //覆盖写  先手动创建文件夹
                BufferedWriter out = new BufferedWriter(new FileWriter("./tmp.cacheRecovery"));
                out.write(itemJSONObj.toString());
                out.close();
            } catch (IOException e) {
                System.out.println("关闭程序时保存缓存信息异常." + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
//        Map<String,String> testkv = new HashMap<>();
//        testkv.put("FieldA","1000001");
//        testkv.put("FieldB","true");
//        testkv.put("TTTTT","WANGXIAO");
//        testkv.put("_fdsf","hello");
//        testkv.put("_event_","row_insert");
//        CacheRecovery.getInstance().add(1,"zdz:account_balance_20181121","1000001",testkv);
//        CacheRecovery.getInstance().show();
        System.out.println("--------------------------------");
        CacheRecovery.getInstance().save();
//        testkv.put("FieldB","false");
//        testkv.put("_event_","row_update");
//        CacheRecovery.getInstance().add(2,"zdz:account_balance_20181121","1000001",testkv);
//        CacheRecovery.getInstance().delOneEle("zdz:account_balance_20181121_1","1000001");
//        CacheRecovery.getInstance().show();
//        Map<String,Map<String,InsertExceptionRecord>> hh = CacheRecovery.getInstance().getOne("zdz:account_balance_20181121-1");
//        Map<String,InsertExceptionRecord> mm = hh.get("1000001");
//        CacheRecovery.getInstance().clear();
//        System.out.println("=====================");
//        CacheRecovery.getInstance().show();
//        System.out.println("=====================");
//        System.out.println(hh.toString());
//        System.out.println(mm.toString());
//        System.out.println("-------------------");
//        CacheRecovery.getInstance().add(1,"zdz:account_balance_20181121","1000001",testkv);
//        CacheRecovery.getInstance().show();
//        Map<String,Map<String,Map<String,InsertExceptionRecord>>> hs = CacheRecovery.getInstance().deepCopy();
//        CacheRecovery.getInstance().show(hs);

    }

    /*
* 将时间转换为时间戳
*/
    public static long dateToStamp(String infoForException, String s){
        long res = System.currentTimeMillis();;
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = simpleDateFormat.parse(s);
            long ts = date.getTime();
            res = Long.valueOf(String.valueOf(ts));
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("时间戳异常:" + infoForException + "时间戳:" + s);
        }
        return res;
    }


}
