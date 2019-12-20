package com.sync.timer;

import com.db.ConnectionPool;
import com.db.ConnectionPoolUtils;
import com.sync.common.FuncParams;
import com.sync.common.GetProperties;
import com.sync.common.HBaseConnection;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 * Created by WangXiao on 2018/11/15.
 * 汇总Sls各个分片中的统计数据
 * 每分钟汇总一次统计值存入rds中
 */
public class SummarySLSShardsData implements Runnable {
    private final static Logger logger = Logger.getLogger(SummarySLSShardsData.class);
    //sls消费客户端ID
    private String client_id = GetProperties.hbase_table_pk.getProperty("sls_client_id");
    //汇总值记录的表名(汇总到hbase时使用，目前是汇总到mysql)
    //private String summary_table = GetProperties.hbase_table_pk.getProperty("summary_table");
    //汇总值来源表
    private String realtime_statistic_table = GetProperties.hbase_table_pk.getProperty("realtime_statistic_table");
    private final Long waitLoopTime = 60L;

    public void summary(String summary_date){
        Long mcd_count = 0L;
        Long mcd_count_s = 0L;
        Long gfd_flow_sum = 0L;
        Long efd_order_sum = 0L;
        Long efd_order_sum_s = 0L;
        Long efd_order_sum_f = 0L;
        Long efd_flow_sum = 0L;
        Long efd_flow_sum_s = 0L;
        Long efd_flow_sum_f = 0L;
        Long luckier_chinamobile = 0L;
        Long luckier_alipay = 0L;
        int i = 0;
        //sls最大支持64个分片
        for (i = 0; i < 64; i++) {
            String rowKey = summary_date + "_" + client_id + "_" + i;
            Result result = null;
            try {
                //hbase连接正常情况下， result永远!=null
                result = HBaseConnection.getInstance().getData(realtime_statistic_table, rowKey, null, null);
            }catch (Exception e){
                System.out.println("无法读取表" + realtime_statistic_table + "的" + rowKey + "记录簇.最终统计记录可能有问题");
                e.printStackTrace();
                result = null;
            }
            if (result == null)
                continue;
            Cell[] cells = result.rawCells();
//            if (cells.length == 0){
//                System.out.println(summary_date + ":共统计" + (i-1) + "个记录(分片).");
//                break;
//            }
            for (Cell cell : cells) {
                if ((new String(CellUtil.cloneQualifier(cell))).equals("mcd_count")){
                    mcd_count = mcd_count + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("mcd_count_s")){
                    mcd_count_s = mcd_count_s + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("gfd_flow_sum")){
                    gfd_flow_sum = gfd_flow_sum + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("efd_order_sum")){
                    efd_order_sum = efd_order_sum + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("efd_order_sum_s")){
                    efd_order_sum_s = efd_order_sum_s + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("efd_order_sum_f")){
                    efd_order_sum_f = efd_order_sum_f + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("efd_flow_sum")){
                    efd_flow_sum = efd_flow_sum + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("efd_flow_sum_s")){
                    efd_flow_sum_s = efd_flow_sum_s + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("efd_flow_sum_f")){
                    efd_flow_sum_f = efd_flow_sum_f + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("luckier_chinamobile")){
                    luckier_chinamobile = luckier_chinamobile + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
                if ((new String(CellUtil.cloneQualifier(cell))).equals("luckier_alipay")){
                    luckier_alipay = luckier_alipay + Long.valueOf( new String(CellUtil.cloneValue(cell)));
                }
            }
        }
        ConnectionPool connPool = ConnectionPoolUtils.GetPoolInstance();
        Connection conn = null;
        try {
            conn = connPool.getConnection();
        }catch (Exception e){
            conn = null;
            System.out.println("获取统计数据库连接异常." + e.getMessage());
            e.printStackTrace();
        }
        if(null == conn){
            return;
        }
        try {
            String source = "hbase_" + client_id;
            String sql_count = "select count(*) as total from activity_track_today_hour where source=\'" + source + "\' and statis_hour=\'" + summary_date + "\'";
            int currentDateRecordNum = 0;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql_count);
            if (rs.next()) {
                currentDateRecordNum = rs.getInt("total");
            }
            rs.close();
            if (0 == currentDateRecordNum){
                //insert
                String sql_insert = String.format("insert into activity_track_today_hour(source,record_status,statis_hour,mcd_count,mcd_count_s,gfd_flow_sum,efd_order_sum,efd_flow_sum,efd_order_sum_s,efd_flow_sum_s,efd_order_sum_f,efd_flow_sum_f,luckier_chinamobile,luckier_alipay) " +
                                "values('%s',0,'%s',%d,%d,%f,%d,%f,%d,%f,%d,%f,%d,%d)",
                        source,summary_date,
                        mcd_count,mcd_count_s,gfd_flow_sum.floatValue(),efd_order_sum,efd_flow_sum.floatValue(),
                        efd_order_sum_s,efd_flow_sum_s.floatValue(),efd_order_sum_f,efd_flow_sum_f.floatValue(),
                        luckier_chinamobile,luckier_alipay);
                stmt.executeUpdate(sql_insert);
            }else{
                //update  条件加上mcd_count<=当前计算值 防止异常无数据显示  后期有可能会成为一个bug. 后人需谨慎
                String sql_update = String.format("update activity_track_today_hour " +
                                "set mcd_count=%d," +
                                "mcd_count_s=%d," +
                                "gfd_flow_sum=%f," +
                                "efd_order_sum=%d," +
                                "efd_flow_sum=%f," +
                                "efd_order_sum_s=%d," +
                                "efd_flow_sum_s=%f," +
                                "efd_order_sum_f=%d," +
                                "efd_flow_sum_f=%f," +
                                "luckier_chinamobile=%d," +
                                "luckier_alipay=%d " +
                                "where source='%s' and statis_hour='%s' and mcd_count<=%d",
                        mcd_count,mcd_count_s,gfd_flow_sum.floatValue(),efd_order_sum,efd_flow_sum.floatValue(),efd_order_sum_s,efd_flow_sum_s.floatValue(),
                        efd_order_sum_f,efd_flow_sum_f.floatValue(),luckier_chinamobile,luckier_alipay,source,summary_date,mcd_count);
                stmt.executeUpdate(sql_update);
            }
            stmt.close();

        }catch (SQLException e){
            System.out.println("sql语句执行异常." + e.getMessage());
        }finally {
            if (null != conn){
                connPool.returnConnection(conn);
            }
        }

//以下代码是将最终结果插入hbase
//        String cacheForException = "";
//        String rowkey = summary_date+"_"+client_id;
//        Put summaryRecord = new Put(Bytes.toBytes(rowkey));
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("statis_day"),Bytes.toBytes(summary_date));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "statis_day" +"\',\'" + summary_date + "\'\n";
//        SimpleDateFormat temp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String gmt_modified = temp.format(new Date());
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gmt_modified"),Bytes.toBytes(gmt_modified));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "gmt_modified" +"\',\'" + gmt_modified + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("mcd_count"),Bytes.toBytes(mcd_count.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "mcd_count" +"\',\'" + mcd_count + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("gfd_flow_sum"),Bytes.toBytes(gfd_flow_sum.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "gfd_flow_sum" +"\',\'" + gfd_flow_sum + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("efd_order_sum"),Bytes.toBytes(efd_order_sum.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "efd_order_sum" +"\',\'" + efd_order_sum + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("efd_order_sum_s"),Bytes.toBytes(efd_order_sum_s.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "efd_order_sum_s" +"\',\'" + efd_order_sum_s + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("efd_order_sum_f"),Bytes.toBytes(efd_order_sum_f.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "efd_order_sum_f" +"\',\'" + efd_order_sum_f + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("efd_flow_sum"),Bytes.toBytes(efd_flow_sum.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "efd_flow_sum" +"\',\'" + efd_flow_sum + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("efd_flow_sum_s"),Bytes.toBytes(efd_flow_sum_s.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "efd_flow_sum_s" +"\',\'" + efd_flow_sum_s + "\'\n";
//        summaryRecord.addColumn(Bytes.toBytes("info"),Bytes.toBytes("efd_flow_sum_f"),Bytes.toBytes(efd_flow_sum_f.toString()));
//        cacheForException = cacheForException + "put \'" + summary_table +"\',\'" + rowkey + "\',\'info:" + "efd_flow_sum_f" +"\',\'" + efd_flow_sum_f + "\'\n";
//
//        try {
//            HBaseConnection.getInstance().insertPut(summary_table, summaryRecord);
//        }catch (Exception e){
//            System.out.println("Error:汇总统计值入表失败，需要人工操作." + e.getMessage());
//            e.printStackTrace();
//            System.out.println("请在hbse shell下手动执行以下命令:\n" + cacheForException);
//            System.out.println("------------------------------------");
//        }
    }

    public void run(){
//        //首先构建当天和前一天的表
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
//        String lastDayTime = df.format(new Date().getTime() - 86400000L);
//        //程序启动时是否先自行执行一次：统计昨天的记录
//        if (null != is_client_started_run_once && is_client_started_run_once.equals("true"))
//            this.summary(lastDayTime);
        if(FuncParams.getInstance().checkBuLuFunc() ||
                !GetProperties.hbase_table_pk.getProperty("statistic_switch").equals("on")){
            System.out.println("本次启动不参与统计逻辑.");
            return;
        }

        while(true){
            try {
//                SimpleDateFormat temp = new SimpleDateFormat("yyyy-MM-dd HH");
//                String currentHour = temp.format(new Date().getTime());
                this.summary(FuncParams.getInstance().getReadingPosition());
                Thread.sleep(this.waitLoopTime*1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        ConnectionPool connPool = ConnectionPoolUtils.GetPoolInstance();
        Connection conn = null;
        try {
            conn = connPool.getConnection();
        }catch (Exception e){
            conn = null;
            System.out.println("获取统计数据库连接异常." + e.getMessage());
            e.printStackTrace();
        }
        if(null == conn){
            return;
        }
        try {
            Long mcd_count = 0L;
            Long mcd_count_s = 1L;
            Long gfd_flow_sum = 0L;
            Long efd_order_sum = 0L;
            Long efd_order_sum_s = 0L;
            Long efd_order_sum_f = 0L;
            Long efd_flow_sum = 0L;
            Long efd_flow_sum_s = 0L;
            Long efd_flow_sum_f = 1L;
            Long luckier_chinamobile = 1L;
            Long luckier_alipay = 1L;
            String summary_date = "2018-11-15";
            String client_id = "1001";
            String source = "hbase_" + client_id;
            String sql_getRow = "select * from activity_track_today where source=\'" + source + "\'";
            String sql_insert = String.format("insert into activity_track_today_hour(source,record_status,statis_hour,mcd_count,mcd_count_s,gfd_flow_sum,efd_order_sum,efd_flow_sum,efd_order_sum_s,efd_flow_sum_s,efd_order_sum_f,efd_flow_sum_f,luckier_chinamobile,luckier_alipay) " +
                            "values('%s',0,'%s',%d,%d,%f,%d,%f,%d,%f,%d,%f,%d,%d)",
                    source,summary_date,
                    mcd_count,mcd_count_s,gfd_flow_sum.floatValue(),efd_order_sum,efd_flow_sum.floatValue(),
                    efd_order_sum_s,efd_flow_sum_s.floatValue(),efd_order_sum_f,efd_flow_sum_f.floatValue(),
                    luckier_chinamobile,luckier_alipay);
            String sql_update = String.format("update activity_track_today_hour " +
                            "set mcd_count=%d," +
                            "mcd_count_s=%d," +
                            "gfd_flow_sum=%f," +
                            "efd_order_sum=%d," +
                            "efd_flow_sum=%f," +
                            "efd_order_sum_s=%d," +
                            "efd_flow_sum_s=%f," +
                            "efd_order_sum_f=%d," +
                            "efd_flow_sum_f=%f," +
                            "luckier_chinamobile=%d," +
                            "luckier_alipay=%d " +
                            "where source='%s' and statis_hour='%s'",
                    mcd_count,mcd_count_s,gfd_flow_sum.floatValue(),efd_order_sum,efd_flow_sum.floatValue(),efd_order_sum_s,efd_flow_sum_s.floatValue(),
                    efd_order_sum_f,efd_flow_sum_f.floatValue(),luckier_chinamobile,luckier_alipay,source,summary_date);
            Statement stmt = conn.createStatement();
            //ResultSet rs = stmt.executeQuery(sql_getRow);
            System.out.println(stmt.executeUpdate(sql_update));


        }catch (SQLException e){
            System.out.println("sql语句执行异常." + e.getMessage());
        }finally {
            if (null != conn){
                try {
                    conn.close();
                }catch (Exception e){e.printStackTrace();}
            }
        }
    }
}
