package com.sync.process;

import com.alarm.Alarming;
import com.alarm.Dingding;
import com.aliyun.openservices.log.common.*;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;
import com.realtimestatistics.CacheRecovery;
import com.realtimestatistics.Statistics_ZongduiZong;
import com.sync.common.Filter;
import com.sync.common.FuncParams;
import com.sync.common.GetProperties;
import com.sync.common.HBaseConnection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

/**
 * Created by WangXiao on 2018/11/6.
 * 阿里云SLS消费者开发文档:https://help.aliyun.com/document_detail/28998.html?spm=a2c4g.11186623.6.796.65a46cc046zBKo
 *
 */
public class SLS2EMRHbase implements ILogHubProcessor
{
    private final static Logger logger = Logger.getLogger(SLS2EMRHbase.class);
    private int mShardId;
    // 记录上次持久化 check point 的时间
    private long mLastCheckTime = 0;
    //sls客户端ID
    private String client_id = GetProperties.hbase_table_pk.getProperty("sls_client_id");
    //hase命名空间
    private String hbase_user_name = GetProperties.hbase_table_pk.getProperty("hbase_user_name");
    //按天分表的表名称
    private String is_new_name_table = GetProperties.hbase_table_pk.getProperty("new_name_table");
    //实时累加表的名字
    private String realtime_statistic_table = GetProperties.hbase_table_pk.getProperty("realtime_statistic_table");
    //统计开关
    private String statisticSwitch = GetProperties.hbase_table_pk.getProperty("statistic_switch");
    //统计数据本地缓存
    private Statistics_ZongduiZong statistics_zdz = null;
    //批量写入hbase (ArrayList线程不安全)
    private Map<String,CopyOnWriteArrayList<Put>> batch = null;
    //间隔一定时间插入hbase一次 中间变量
    private long batchInsertTime = 0L;
    //时间间隔 s
    private int inserSpan = Integer.valueOf(GetProperties.hbase_table_pk.getProperty("insert_span"));
    //运行模式 normal or bulu
    private boolean runMode_bulu = false;
    //补录模式 过滤
    private Filter filter = null;

    public void initialize(int shardId)
    {
        mShardId = shardId;
        //查看统计开关  常规模式下statisticSwitch配置才生效
        runMode_bulu = FuncParams.getInstance().checkBuLuFunc();
        if (!runMode_bulu){
            if (statisticSwitch != null || statisticSwitch.equals("on")){
                statistics_zdz = new Statistics_ZongduiZong(FuncParams.getInstance().getStartPosition("yyyy-MM-dd HH"),
                        client_id,String.valueOf(shardId));
            }
        }else{
            filter = new Filter();
        }
        batch = new HashMap<>();
        //将记录内存块引用到缓存中，便于关闭启动程序时恢复记录
        CacheRecovery.getInstance().referBatchBlcok(shardId,batch);
        KillHandler.getInstance().register(String.valueOf(shardId));
    }

    /**
     * 将每条binlog存入内存中
     * @param tableName  hbase表名
     * @param put 具体改动内容
     */
    private void packBatch(String tableName, Put put){
        put.setDurability(Durability.SKIP_WAL);
        CopyOnWriteArrayList<Put> putList = batch.get(tableName);
        if (null == putList){
            putList = new CopyOnWriteArrayList<>();
            batch.put(tableName,putList);
        }
        putList.add(put);
    }

    /**
     * 将内存中攒的记录一次性插入到hbase
     * 间隔时间inserSpan由外部配置
     */
    private void insertHbaseBatch(){
        long nowTime = System.currentTimeMillis();
        //每隔 ?s写入hbase
        if (nowTime - batchInsertTime < inserSpan*1000){
            return;
        }
        for (Map.Entry<String,CopyOnWriteArrayList<Put>> records : batch.entrySet()){
            try {
                if (records.getValue().size() > 0) {
                    HBaseConnection.getInstance().insertBatch(records.getKey(), records.getValue());
                    //logger.info("写入Hbase成功:" + records.getKey());
                }
            } catch (Exception e) {
                System.out.println(String.valueOf(nowTime) + " 批量插入异常:" + e.getMessage());
                CacheRecovery.getInstance().add(mShardId,records.getKey(),records.getValue());
                logger.info("Hbase写入异常:" + records.getKey());
            }finally {
                records.getValue().clear();
            }
        }

        //重置插入hbase时间点
        batchInsertTime = nowTime;
        //清理表 因为有按天生成的表，所以需要清理，会越来越多.本不想清理
        batch.clear();
    }

    // 消费数据的主逻辑，这里面的所有异常都需要捕获，不能抛出去。
    public String process(List<LogGroupData> logGroups,
                          ILogHubCheckPointTracker checkPointTracker)
    {
        try {
            // 这里简单的将获取到的数据打印出来
            for (LogGroupData logGroup : logGroups) {
                FastLogGroup flg = logGroup.GetFastLogGroup();
//            System.out.println(String.format("\tcategory\t:\t%s\n\tsource\t:\t%s\n\ttopic\t:\t%s\n\tmachineUUID\t:\t%s",
//                    flg.getCategory(), flg.getSource(), flg.getTopic(), flg.getMachineUUID()));
//            System.out.println("Tags");
//            for (int tagIdx = 0; tagIdx < flg.getLogTagsCount(); ++tagIdx) {
//                FastLogTag logtag = flg.getLogTags(tagIdx);
//                System.out.println(String.format("\t%s\t:\t%s", logtag.getKey(), logtag.getValue()));
//            }
                for (int lIdx = 0; lIdx < flg.getLogsCount(); ++lIdx) {
                    FastLog log = flg.getLogs(lIdx);
                    //System.out.println("--------\nLog: " + lIdx + ", time: " + log.getTime() + ", GetContentCount: " + log.getContentsCount());
                    Map<String, String> tempKV = new HashMap<>();
                    for (int cIdx = 0; cIdx < log.getContentsCount(); ++cIdx) {
                        FastLogContent content = log.getContents(cIdx);
                        tempKV.put(content.getKey(), content.getValue());
                        //System.out.println(content.getKey() + "\t:\t" + content.getValue());
                    }
                    //过滤历史数据 begin 防止RDS主备切binlog变动时sls
                    // TODO: 2018/11/16 过滤历史binlog
                    //过滤历史数据 end
                    String tableName = tempKV.get("_table_");
                    //如果是补录模式，根据黑白名单过滤表
                    if (runMode_bulu){
                        if (!filter.isMatch(tableName))
                            continue;
                    }
                    //去除分表标识的表名字
                    String pure_tableName = null;
                    //实际要写入hbase的表名字
                    String real_tableName = null;
                    pure_tableName = getCombinedTableName(tableName, "_");
                    real_tableName = pure_tableName;
                    // TODO: 2019/1/14 封装rowkey生成 主键、联合主键、倒叙、md5等
                    String pk = GetProperties.hbase_table_pk.getProperty(real_tableName + "_primarykey");
                    if (null == pk || "".equals(pk)){
                        logger.warn("表:" + real_tableName + " 未配置主键");
                        Alarming.getInstance().add("sls-hbase##" + "表:" + real_tableName + " 未配置主键");
                        continue;
                    }
                    String[] ps_array = pk.split(",");
                    String rowkey = "";
                    for (int t_index = 0; t_index<ps_array.length; t_index++){
                        String one_key = tempKV.get(ps_array[t_index]);
                        if (null==one_key || "".equals(one_key)){
                            rowkey = "";
                            logger.error("表:" + real_tableName + " 主键配置错误:" + ps_array[t_index]);
                            Alarming.getInstance().add("sls-hbase##" + "表:" + real_tableName + " 主键配置错误");
                            break;
                        }
                        rowkey = rowkey + one_key + "_";
                    }
                    if ("".equals(rowkey)){
                        continue;
                    }else{
                        rowkey = rowkey.substring(0,rowkey.length()-1);
                    }
                    //String rowkey = tempKV.get(GetProperties.hbase_table_pk.getProperty(real_tableName + "_primarykey"));
                    //特殊处理 记账表按天记录 account_balance
                    if (is_new_name_table != null && is_new_name_table.indexOf(real_tableName) >= 0 && !real_tableName.equals("account")) {
                        String ab_gmt_create = tempKV.get("gmt_create").substring(0, 10).replaceAll("-", "");
                        real_tableName = real_tableName + "_" + ab_gmt_create;

                        //因记账业务逻辑修改而必须追加写的代码-------------------------------------------------------begin
                        SimpleDateFormat ft = new SimpleDateFormat ("yyyyMMdd");
                        Date temp_now_date = null;
                        if(this.runMode_bulu){
                            temp_now_date = ft.parse(FuncParams.getInstance().getStartPosition("yyyy-MM-dd HH:mm:ss").substring(0, 10).replaceAll("-", ""));
                        }else{
                            temp_now_date = new Date();
                        }
                        if (!ab_gmt_create.equals(ft.format(temp_now_date))){
                            real_tableName = "history_update_account_balance" + "_" + tempKV.get("gmt_modified").substring(0, 10).replaceAll("-", "");
                        }
                        //因记账业务逻辑修改而必须追加写的代码-------------------------------------------------------end
                    }
                    //缓存失败插入记录，方便人工插入hbase
                    String cacheForException = "";
                    if (tempKV.get("_event_").equals("row_insert") ||
                            tempKV.get("_event_").equals("row_update")) {
                        //统计逻辑在这里 begin
                        Put statisticPut = null;
                        if (statistics_zdz != null)
                            statisticPut = statistics_zdz.compute(pure_tableName, client_id, Integer.toString(mShardId), tempKV);
                        //统计逻辑在这里 end
                        long current_timestamp = dateToStamp(tableName + ":" + rowkey,tempKV.get("gmt_modified"));
                        //logger.info(tempKV.get("_event_") + ": " + tableName + "有记录改动");
                        Put put = null;
                        if(rowkey!=null) {
                            put = new Put(Bytes.toBytes(rowkey));
                            for (Map.Entry<String, String> entryset : tempKV.entrySet()) {
                                if (entryset.getKey().indexOf("_") == 0 || entryset.getValue() == null)
                                    continue;
                                if (tempKV.get("_event_").equals("row_update") &&
                                        entryset.getValue().equals(tempKV.get("_old_" + entryset.getKey())))
                                    continue;
                                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entryset.getKey()), current_timestamp, Bytes.toBytes(entryset.getValue()));
                                cacheForException = cacheForException + "put \'" + hbase_user_name + ":" + real_tableName + "\',\'" + rowkey + "\',\'info:" + entryset.getKey() + "\',\'" + entryset.getValue() + "\';";
                            }

                            packBatch(hbase_user_name + ":" + real_tableName, put);
                        }else{
                            logger.warn("又添加新表了:" + pure_tableName);
                            Alarming.getInstance().add("sls-hbase##" + "又添加新表了:" + pure_tableName);
                        }
//                        try {
//                            HBaseConnection.getInstance().insertPut(hbase_user_name + ":" + real_tableName, put);
//                        } catch (Exception e) {
//                            CacheRecovery.getInstance().add(mShardId,hbase_user_name + ":" + real_tableName,rowkey,tempKV);
////                            logger.error("修改hbase表" + tableName + "时有异常" + e.getMessage()
////                                    + "  classname:" + e.getClass().getTypeName());
//                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                            String currentTime = df.format(new Date());
////                            System.out.println(currentTime + "  修改hbase表" + tableName + "时有异常:" + e.getMessage()
////                                    + "  classname:" + e.getClass().getTypeName());
////                            e.printStackTrace();
//                            System.out.println(currentTime + " 插入异常,需要手动执行:\n" + cacheForException);
//                            System.out.println("------------------------------------");
//                        }
                        try {
                            if (null != statisticPut)
                                HBaseConnection.getInstance().insertPut(realtime_statistic_table, statisticPut);
                        } catch (Exception e) {
                            System.out.println("统计信息入hbase失败...: " + e.getMessage());
                            logger.info("UnkownError: 统计信息入hbase失败." + e.getMessage());
                            Alarming.getInstance().add("sls-hbase##" + "UnkownError: 统计信息入hbase失败.");
                            e.printStackTrace();
                        }
                    }
                }
            }
        }catch(Exception ee){
            ee.printStackTrace();
            logger.error("UnkownError: 主函数异常." + ee.getMessage());
            Alarming.getInstance().add("sls-hbase##" + "UnkownError: 主函数异常.");
            System.out.println("捕获到这个异常了:" + ee.getMessage());
        }
        insertHbaseBatch();
        KillHandler.getInstance().checkProcessEnd(String.valueOf(mShardId));
        long curTime = System.currentTimeMillis();
        // 每隔 30 秒，写一次 check point 到服务端，如果 30 秒内，worker crash，
        // 新启动的 worker 会从上一个 checkpoint 其消费数据，有可能有少量的重复数据
        if (curTime - mLastCheckTime >  30 * 1000)
        {
            try
            {
                //参数true表示立即将checkpoint更新到服务端，为false会将checkpoint缓存在本地，后台默认隔60s会将checkpoint刷新到服务端。
                checkPointTracker.saveCheckPoint(true);
            }
            catch (LogHubCheckPointException e)
            {
                e.printStackTrace();
            }
            mLastCheckTime = curTime;
        }
        return null;
    }
    // 当 worker 退出的时候，会调用该函数，用户可以在此处做些清理工作。
    public void shutdown(ILogHubCheckPointTracker checkPointTracker)
    {
        //将消费断点保存到服务端。
        try {
            checkPointTracker.saveCheckPoint(true);
        } catch (LogHubCheckPointException e) {
            e.printStackTrace();
        }
    }
    private String getCombinedTableName(String originTableName, String splitChar){
        String tableName = "";
        String temp_splitChar = "_";
        if (splitChar != null && !"".equals(splitChar)) {
            temp_splitChar = splitChar;
        }
        String[] parts = originTableName.split(temp_splitChar);
        Pattern pattern = Pattern.compile("[0-9]*");
        if(pattern.matcher(parts[parts.length-1]).matches()){
            for (int i = 0; i < parts.length - 1; i++){
                tableName = tableName + "_" + parts[i];
            }
            tableName = tableName.substring(tableName.indexOf(temp_splitChar)+1,tableName.length());
        }else{
            tableName = originTableName;
        }

        return tableName;
    }

    /**
     * 将各个分片的位点信息保存到本地文件
     */
    private void saveCheckPoitToNative(){
        try {
            //覆盖写  先手动创建文件夹
            BufferedWriter out = new BufferedWriter(new FileWriter("./chechpoint/Shard_" + mShardId));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String chekpoint = sdf.format(new Date(Long.valueOf(mLastCheckTime)));
            out.write(chekpoint);
            out.close();
        } catch (IOException e) {
        }
    }

    /*
* 将时间转换为时间戳
*/
    public long dateToStamp(String infoForException, String s){
        long res = System.currentTimeMillis();;
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
            System.out.println("时间戳异常:" + infoForException + "时间戳:" + s);
        }
        return res;
    }
}
class SLS2EMRHbaseFactory implements ILogHubProcessorFactory
{
    public ILogHubProcessor generatorProcessor()
    {
        // 生成一个消费实例
        return new SLS2EMRHbase();
    }
}