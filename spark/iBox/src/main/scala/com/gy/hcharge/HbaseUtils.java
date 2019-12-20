package com.gy.hcharge;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;

import java.util.concurrent.ExecutorService;

import java.io.IOException;



/**
 * Created by Administrator on 2019/11/16 0016.
 */
public class HbaseUtils {

    public static Configuration conf = null;
    public static ExecutorService executor = null;
    public static Admin admin = null;
    public static Connection conn = null;
    public static AggregationClient  aggregationClient= null;
    static {
        //1. 获取连接配置对象
        conf = HBaseConfiguration.create();
        //2. 设置连接hbase的参数
        conf.set("hbase.zookeeper.quorum", ConfigFactory.hbasezookeeper);
        //3. 获取Admin对象
        try {
            conn = Hbase.getHBaseConn("dx_intelli_recharge@BIGDATA1.COM",ConfigFactory.confPath +"dx_intelli_recharge.keytab");
            admin = conn.getAdmin();
            conf = conn.getConfiguration();
            aggregationClient = new AggregationClient(conf);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static AggregationClient getAggregationClient(){return aggregationClient;}
    public static Configuration getHbaseConf(){return conf;}
    public static Admin getHbaseAdmin(){return admin;}


    public static void closeConn()throws IOException{
        if(conn!=null)
            conn.close();
    }

    public static void close(Admin admin){
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void close(Admin admin,Table table){
        try {
            if(admin!=null) {
                admin.close();
            }
            if(table!=null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(Table table){
        try {
            if(table!=null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table getTable(TableName tableName) throws IOException {
        if(admin.tableExists(tableName))
            return conn.getTable(tableName);
        else{
            return null;
        }
    }




    public static boolean createNewTable(String table){
        boolean flag = false;
        try{
            TableName tableName = TableName.valueOf(table);
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            descriptor.addFamily(new HColumnDescriptor("non"));
            admin.createTable(descriptor);
            flag = true;
            //admin.enableTable(tableName);
        }catch (IOException e){e.printStackTrace();}
            return flag;

    }
    public static void truncateTable(String table){
        //System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
        boolean flag = false;
        try{
            TableName tableName = TableName.valueOf(table);
            if(admin.tableExists(tableName)){
                if(admin.isTableDisabled(tableName)){
                    admin.deleteTable(tableName);
                }else{
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                }
                flag = createNewTable(table);
            }else{
                flag = createNewTable(table);
            }


        }catch (IOException e){e.printStackTrace();}
        return;
    }

}
