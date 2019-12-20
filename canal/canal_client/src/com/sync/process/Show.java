package com.sync.process;

import com.alarm.Alarming;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.aliyun.openservices.log.common.FastLogContent;
import com.realtimestatistics.CacheRecovery;
import com.switchfield.SwitchContext;
import com.sync.common.GetProperties;
import com.sync.common.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by WangXiao on 2018/11/1.
 */
public class Show implements Runnable {
    private final static Logger logger = Logger.getLogger(Show.class);
    private CanalConnector connector = null;
    private String thread_name = null;
    private SwitchContext switchContext = new SwitchContext();

    public void process() {
        int batchSize = 1000;
        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(GetProperties.canal.ip, GetProperties.canal.port), GetProperties.canal.destination[0],
                GetProperties.canal.username, GetProperties.canal.password);

        connector.connect();
        System.out.println("Filter=" + GetProperties.canal.filter);
        if (!"".equals(GetProperties.canal.filter)) {
            connector.subscribe(GetProperties.canal.filter);

        } else {
            connector.subscribe();
        }
        connector.rollback();

        try {
            logger.info(thread_name + " canal client 启动成功");
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // get batch num
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (!(batchId == -1 || size == 0)) {
                    if (syncEntry(message.getEntries())) {
                        connector.ack(batchId); // commit
                    } else {
                        connector.rollback(batchId); // rollback
                    }
                }else{
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
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(
                        thread_name + "parser of eromanga-event has an error , data:" + entry.toString(), e);
            }
            CanalEntry.EventType eventType = rowChage.getEventType();
            long no = entry.getHeader().getLogfileOffset();

            String tableName = getCombinedTableName(entry.getHeader().getTableName(),"_");

            System.out.println("----------------------------------");
            if (eventType == CanalEntry.EventType.DELETE){
                logger.info("表:" + tableName + " 有记录删除");
                System.out.println("表:" + tableName + " 有记录删除");
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("1111111111111111111111111111111");
                }
            }else if(eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE){
                logger.info("表:" + tableName + " 有记录修改");
                System.out.println("表:" + tableName + " 有记录修改");
                String rowkey = null;
                String _event_ = null;
                if (eventType == CanalEntry.EventType.INSERT){
                    _event_ = "row_insert";
                }else{
                    _event_ = "row_update";
                }
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if(null == rowkey){
                        rowkey = getTablePK(rowData.getAfterColumnsList(), GetProperties.hbase_table_pk.getProperty(tableName));
                        System.out.println("rowkey====" + rowkey);
                    }
                    Map<String,String> tempKV = new HashMap<>();
                    String cacheForException = "";
                    List<CanalEntry.Column> lc = rowData.getAfterColumnsList();
                    for (CanalEntry.Column content : lc) {
                        tempKV.put(content.getName(),content.getValue());
                        String newValue = this.switchContext.getNewValue(tableName,content.getName(),content.getValue());
                        System.out.println(content.getName() + "\t:\t" + newValue);
                    }
                    tempKV.put("_event_",_event_);
                    printColumn(rowData.getAfterColumnsList());
                }
            }else{
                logger.info("暂时还不支持该canal类型操作 类型名:" + eventType.name());
                continue;
            }
        }
        return ret;
    }



    private Map<String, Object> makeColumn(List<CanalEntry.Column> columns) {
        Map<String, Object> one = new HashMap<String, Object>();
        for (CanalEntry.Column column : columns) {
            one.put(column.getName(), column.getValue());
        }
        return one;
    }

    private String getTablePK(List<CanalEntry.Column> columnlist, String tablePKConfig){
        String primarykey = "";
        if (tablePKConfig == null || "".equals(tablePKConfig)){
            for (CanalEntry.Column onecolumn : columnlist){
                if (onecolumn.getIsKey()){
                    primarykey = primarykey + onecolumn.getValue() + "_";
                }
            }
        }else if(tablePKConfig.indexOf(",") > 0){
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
        }else{
            for (CanalEntry.Column onecolumn : columnlist){
                if (tablePKConfig.equals(onecolumn.getName())){
                    primarykey = onecolumn.getValue();
                    break;
                }
            }
        }
        int lastUnderscore = primarykey.lastIndexOf("_");
        if (lastUnderscore == primarykey.length() - 1)
            primarykey = primarykey.substring(0,lastUnderscore);
        return primarykey;
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

    protected void finalize() throws Throwable {
        if (connector != null) {
            connector.disconnect();
            connector = null;
        }
    }
    private void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() +  "(KEY:"+ column.getIsKey() + ")    update=" + column.getUpdated());
            logger.info(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
    private void saveCheckPoitToNative(){
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("./chechpoint/Shard_0"));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String chekpoint = sdf.format(new Date(Long.valueOf("1542785283000")));
            out.write(chekpoint);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }


}

