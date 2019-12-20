package com.sync.process;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.realtimestatistics.CacheRecovery;
import com.switchfield.SwitchContext;
import com.sync.common.GetProperties;
import com.sync.common.HBaseConnection;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

/**
 * Created by WangXiao on 2018/7/27.
 * 从canal中获取的日志转化后直接存入Hbase
 */
public class Hbase implements Runnable {
    private final static Logger logger = Logger.getLogger(Hbase.class);
    private CanalConnector connector = null;
    private String thread_name = null;
    // hbase表的空间名
    private String hbase_user_name = null;
    // instance-yuka，现是单canal实例服务同步
    private String canal_destination = null;
    // binlog的位置信息
    private long start_position = 0L;
    // 用于解析表名（分库分表是时候表的后面都会带 _00000这样子）
    private boolean is_combine_table = false;
    // 批量写入hbase (ArrayList线程不安全)
    private Map<String,CopyOnWriteArrayList<Put>> batch = null;
    // 敏感数据脱敏处理
    private SwitchContext switchContext = new SwitchContext();

    public Hbase(String threadname, String hbaseusername,long startpos, boolean isCombineTable) {
        thread_name = "canal[" + threadname + "]:";
        hbase_user_name = hbaseusername;
        // instance-yuka，现是单canal实例服务同步
        canal_destination = GetProperties.canal.destination[0];
        // 0L
        this.start_position = startpos;
        // true
        is_combine_table = isCombineTable;
        batch = new HashMap<>();
        //将记录内存块引用到缓存中，便于关闭启动程序时恢复记录
        CacheRecovery.getInstance().referBatchBlcok(0,batch);
        KillHandler.getInstance().register(Thread.currentThread().getName());
    }
    public void process() {
        int batchSize = 1000;

        // 1. 获取canal-server连接实例
        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(GetProperties.canal.ip, GetProperties.canal.port), GetProperties.canal.destination[0],
                GetProperties.canal.username, GetProperties.canal.password);

        connector.connect();

        // 2.filter为空
        if (!"".equals(GetProperties.canal.filter)) {
            connector.subscribe(GetProperties.canal.filter);
        } else {
            // 订阅消费
            connector.subscribe();
        }

        //  回滚上次的get请求，重新获取数据。基于get获取的batchId进行提交，避免误操作
        connector.rollback();

        try {
            while (true) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize); // get batch num
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (!(batchId == -1 || size == 0)) { // 不是空数据
                    // 提交确认
                    if (syncEntry(message.getEntries())) {
                        connector.ack(batchId);      // commit
                    } else {
                        connector.rollback(batchId); // rollback
                    }
                }else{
                    // 空数据的情况下睡眠
                    try {
                        logger.info("暂时没有消息...");
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            if (connector != null) {
                connector.disconnect();
                connector = null;
            }
        }
    }

    public void run() {
        while (true) {
            try {
                process();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(thread_name + "canal link failure!");
            }
        }
    }

    private boolean syncEntry(List<CanalEntry.Entry> entrys) {
        RecordMetadata metadata = null;
        boolean ret = true;
        // CanalEntry.Entry entry 是 canal的数据格式
        for (CanalEntry.Entry entry : entrys) {
            // 事务类型：事务开始、事务结束
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // CanalEntry.RowChange 是 CanalEntry.Entry的成员记录了变更操作的具体值
            CanalEntry.RowChange rowChage = null;
            try {
                // storeValue byte数据,可展开，对应的类型为RowChange
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(
                        thread_name + "parser of eromanga-event has an error , data:" + entry.toString(), e);
            }
            // eventType 记录操作类型 [insert/update/delete类型]
            CanalEntry.EventType eventType = rowChage.getEventType();
            // binlog的位置信息
            long no = entry.getHeader().getLogfileOffset();

            // start_position > binlog的要读取的位置信息，表示已经读取过了
            if (start_position > no){
                continue;
            }

            // 获取表名称（组合表判断）
            String tableName = "";
            if (is_combine_table){
                // 分库分表时需要将表名进行拆调0000这些数字
                tableName = getCombinedTableName(entry.getHeader().getTableName(),null);
            }else{
                tableName = entry.getHeader().getTableName();
            }

            // 根据删除记录（单条删除）
            if (eventType == CanalEntry.EventType.DELETE){
                logger.info("表:" + tableName + " 有记录删除");
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    try {
                        HBaseConnection.getInstance().delete(hbase_user_name + ":" + tableName,
                                getTablePK(rowData.getBeforeColumnsList(), GetProperties.hbase_table_pk.getProperty(tableName)),
                                null,
                                null);
                    }catch (Exception e){
                        logger.error("删除表" + tableName + "记录时有异常." + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
            // 更新和插入（批量操作）
            else if(eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE){
                logger.info("表:" + tableName + " 有记录修改");
                String rowkey = null;
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if(null == rowkey){
                        rowkey = getTablePK(rowData.getAfterColumnsList(), GetProperties.hbase_table_pk.getProperty(tableName));
                    }
                    Put put = new Put(Bytes.toBytes(rowkey));
                    // 插入的数据，添加对应的列族等信息
                    // CanalEntry.Column类型，每行的一列数据
                    for (CanalEntry.Column onecolumn : rowData.getAfterColumnsList()){
                        // 是否发生变更，发生变更记录
                        if (!onecolumn.getUpdated())
                            continue;
                        // 数据脱敏处理
                        String newValue = this.switchContext.getNewValue(tableName,onecolumn.getName(),onecolumn.getValue());
                        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(onecolumn.getName()),Bytes.toBytes(newValue));
                    }
                    // 将需要插入的数据存到bath对象中（Hbase.batch）
                    packBatch(hbase_user_name + ":" + tableName,put);
                }
            }else{
                logger.info("暂时还不支持该canal类型操作 类型名:" + eventType.name());
                continue;
            }
        }
        insertHbaseBatch();
        // 是否结束写入线程
        KillHandler.getInstance().checkProcessEnd(Thread.currentThread().getName());
        return ret;
    }

    /**
     * 将内存中攒的记录一次性插入到hbase
     * 间隔时间inserSpan由外部配置
     */
    private void insertHbaseBatch(){
        for (Map.Entry<String,CopyOnWriteArrayList<Put>> records : batch.entrySet()){
            try {
                if (records.getValue().size() > 0) {
                    HBaseConnection.getInstance().insertBatch(records.getKey(), records.getValue());
                    logger.info("写入Hbase成功:" + records.getKey());
                }
            } catch(TableNotFoundException ne){
                logger.info("未同步的表,不做处理：" + records.getKey());
            }catch (Exception e) {
                // 存放插入失败的数据，在另一个线程中处理
                CacheRecovery.getInstance().add(0,records.getKey(),records.getValue());
                logger.info("Hbase写入异常:" + records.getKey());
            }finally {
                records.getValue().clear();
            }
        }
        //清理表 因为有按天生成的表，所以需要清理，会越来越多.本不想清理
        batch.clear();
    }
    /**
     * 将每条binlog存入内存中
     * @param tableName  hbase表名-shardid  举例："zdz:account-0"
     * @param put 具体改动内容
     */
    private void packBatch(String tableName, Put put){
        /*
        整个写请求里，WALEdit对象序列化写入到HLog是唯一会发生I/O的步骤，这个会大大影响写请求的性能。当然，
        如果业务场景对数据稳定性要求不高，关键是写入请求，那么可以调用Put.setDurability(Durability.SKIP_WAL)
         */
        put.setDurability(Durability.SKIP_WAL);
        CopyOnWriteArrayList<Put> putList = batch.get(tableName);
        if (null == putList){
            putList = new CopyOnWriteArrayList<>();
            batch.put(tableName,putList);
        }
        putList.add(put);
    }

    private Map<String, Object> makeColumn(List<CanalEntry.Column> columns) {
        Map<String, Object> one = new HashMap<String, Object>();
        for (CanalEntry.Column column : columns) {
            one.put(column.getName(), column.getValue());
        }
        return one;
    }

    /**
     * rowkey = keyname+keyvalue+"_"
     * @param columnlist
     * @param tablePKConfig
     * @return
     */
    private String getTablePK(List<CanalEntry.Column> columnlist, String tablePKConfig){
        String primarykey = "";

        // 没设置主键的情况
        if (tablePKConfig == null || "".equals(tablePKConfig)){
            for (CanalEntry.Column onecolumn : columnlist){
                if (onecolumn.getIsKey()){
                    primarykey = primarykey + onecolumn.getValue() + "_";
                }
            }
        }
        // 联合主键不能超过3个字段
        else if(tablePKConfig.indexOf(",") > 0){
            String[] pk_array = tablePKConfig.split(",");
            if (pk_array.length > 3){
                logger.info("存在不合理的表主键设计");
            }
            for (String pkname : pk_array){
                for (CanalEntry.Column onecolumn : columnlist){
                    if (pkname.equals(onecolumn.getName())){
                        primarykey = primarykey + onecolumn.getValue() + "_";
                        break;
                    }
                }
            }
        }
        // 单主键
        else{
            for (CanalEntry.Column onecolumn : columnlist){
                if (tablePKConfig.equals(onecolumn.getName())){
                    primarykey = onecolumn.getValue();
                    break;
                }
            }
        }
        // _ 后面的为主键
        int lastUnderscore = primarykey.lastIndexOf("_");
        if (lastUnderscore == primarykey.length() - 1)
            primarykey = primarykey.substring(0,lastUnderscore);
        return primarykey;
    }

    /**
     * 表名拆分
     * @param originTableName
     * @param splitChar
     * @return
     */
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

    protected void finalize() throws Throwable {
        if (connector != null) {
            connector.disconnect();
            connector = null;
        }
    }
    private void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
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

    public static void main(String[] args) {
//        Hbase temp = new Hbase("carowner","car_owner",0L,true);
//        temp.process();

        for (int index = 0; index < 10; index++){
            String threadName = "carowner" + index;
            Thread thread = new Thread(new Hbase(threadName,"car_owner",0L,true));
            thread.run();
        }

        //System.out.println("abc=" + temp.getCombinedTableName("testing-wangxiao_0001",null));
    }

}
