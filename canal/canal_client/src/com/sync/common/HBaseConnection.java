package com.sync.common;

/**
 * Created by WangXiao on 2018/7/26.
 * HBase连接
 */
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseConnection {

    public static Configuration configuration;
    public static Connection connection;

    private static HBaseConnection instance = null;

    public static HBaseConnection getInstance(){
        if (instance == null) {
            synchronized (HBaseConnection.class){
                if (instance == null) {
                    instance = new HBaseConnection();
                }
            }
        }
        return instance;
    }

    private HBaseConnection() {
        // 这个配置文件主要是记录 kerberos的相关配置信息，例如KDC是哪个IP？默认的realm是哪个？
        // 如果没有这个配置文件这边认证的时候肯定不知道KDC的路径喽
        // 这个文件也是从远程服务器上copy下来的
        System.setProperty("java.security.krb5.conf", "krb5.conf");
        configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("hbase-site.xml"));
        configuration.addResource(new Path("core-site.xml"));

        UserGroupInformation.setConfiguration(configuration);
        try {
            UserGroupInformation.loginUserFromKeytab(GetProperties.hbase_table_pk.getProperty("kerberos_user"), "bussiness.keytab" );

            configuration.set("hbase.client.retries.number", "3");
            configuration.set("hbase.rpc.timeout", "20000");
            configuration.set("hbase.client.operation.timeout", "30000");
            configuration.set("hbase.client.scanner.timeout.period", "300000");

            ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池
            connection = ConnectionFactory.createConnection(configuration, pool);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        println("Start...");
        try {
//             String[] familys = {"info"};
//             boolean booleanFlag = true;
//            HBaseConnection.getInstance().creatTable("testing:student", familys, booleanFlag);
//
//            HBaseConnection.getInstance().insert("testing:student", "2018-11-14_1", "info", "cost", "10000");
//            HBaseConnection.getInstance().insert("testing:student", "2018-11-14_1", "info", "save", "99110");
//            HBaseConnection.getInstance().insert("testing:student", "2018-11-14_2", "info", "cost", "20001");
//            HBaseConnection.getInstance().insert("testing:student", "2018-11-14_2", "info", "save", "100");

            //listTables();
            //HBaseConnection.getInstance().deleteTable("testing:student");
            Put put1 = new Put(Bytes.toBytes("first"));
            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), 1L,Bytes.toBytes("wangxiao"));
            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), 2L,Bytes.toBytes("33"));
//            List<Cell> lc = put1.get(Bytes.toBytes("info"),Bytes.toBytes("name"));
//            for (Cell cell : lc) {
//                println("rowkey: " + new String(CellUtil.cloneRow(cell)) + " ");
//                println("Timetamp: " + cell.getTimestamp() + " ");
//                println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
//                println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
//                println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
//                println("--------------------------------");
//            }
            System.out.println(new String(put1.getRow()));
            NavigableMap<byte [], List<Cell>> temp = put1.getFamilyCellMap();
            for (Map.Entry<byte [], List<Cell>> ele : temp.entrySet()){
                System.out.println(new String(ele.getKey()) + ":");
                for (Cell cell : ele.getValue()) {
                    println("rowkey: " + new String(CellUtil.cloneRow(cell)) + " ");
                    println("Timetamp: " + cell.getTimestamp() + " ");
                    println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
                    println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
                    println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
                    println("--------------------------------");
                }
            }



        }catch (Exception e){
            e.printStackTrace();
        }
        println("End...");
    }

    /**
     * 关闭连接
     */
    public static synchronized  void close() {
        try {
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            instance = null;
        }
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param
     * @throws IOException
     */
    /**
     * 创建一张表
     *
     * @param myTableName
     * @param colFamily
     * @param deleteFlag
     *            true:存在则删除再重建
     * @throws Exception
     */
    public static void creatTable(String myTableName, String[] colFamily, boolean deleteFlag) throws Exception {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        TableName tableName = TableName.valueOf(myTableName);
        Admin admin = connection.getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                if (!deleteFlag) {
                    System.out.println(myTableName + " table exists!");
                } else {
                    deleteTable(myTableName); // 先删除原先的表
                    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                    for (String str : colFamily) {
                        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                        hColumnDescriptor.setMaxVersions(10);
                        hTableDescriptor.addFamily(hColumnDescriptor);
                    }
                    admin.createTable(hTableDescriptor);
                    System.out.println(myTableName + "表创建成功。。。");
                }

            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (String str : colFamily) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                    //hColumnDescriptor.setMaxVersions(10); 设置数据最大保存的版本数
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
                System.out.println(myTableName + "表创建成功。。。");
            }
        }finally {
            if(admin != null)
                admin.close();
        }

    }

    /**
     * 删除表
     *
     * @param tableName 表名称
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        TableName tName = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        try {
            if (admin.tableExists(tName)) {
                admin.disableTable(tName);
                admin.deleteTable(tName);
                println(tableName + " is deleted");
            } else {
                println(tableName + " not exists.");
            }
        }finally {
            if(admin != null)
                admin.close();
        }
    }

    /**
     * 查看已有表
     *
     * @throws IOException
     */
    public static void listTables() {
        HTableDescriptor hTableDescriptors[] = null;
        try {
            UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
            Admin admin = connection.getAdmin();
            hTableDescriptors = admin.listTables();
            admin.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            println(hTableDescriptor.getNameAsString());
        }
    }

    /**
     * 插入单行
     *
     * @param tableName 表名称
     * @param rowKey RowKey
     * @param colFamily 列族
     * @param col 列
     * @param value 值
     * @throws IOException
     */
    public static void insert(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);
        table.close();

		/*
		 * 批量插入 List<Put> putList = new ArrayList<Put>(); puts.add(put); table.put(putList);
		 */

        table.close();
    }
    public static void insertPut(String tableName, Put put) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(put);
        table.close();
    }
    public static void inserPuts(String tableName, List<Put> puts) throws Exception{
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(puts);
        table.close();
    }

    public static void insertBatch(String tablename, List<Put> puts) throws Exception{
        // 批量插入失败，监听器
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException{
//                for (int i = 0; i < e.getNumExceptions(); i++) {
//                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
//                }
                throw e;
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024); // 设定阈值 5M 达到5M则提交一次
        // 手动提交，增大数据阈值，能减少提交次数，提高入库效率
        final BufferedMutator mutator = connection.getBufferedMutator(params);
        try {
            mutator.mutate(puts);          // 数据量达到5M时会自动提交一次
            mutator.flush();               // 手动提交一次
        } finally {
            mutator.close();
        }

    }

    /**
     * 根据主键rowKey删除表
     * @param tableName    表名 （must）
     * @param rowKey       主键 （must）
     * @param colFamily    列族
     * @param col          列
     * @throws IOException
     */
    public static void delete(String tableName, String rowKey, String colFamily, String col) throws IOException {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            println(tableName + " not exists.");
        } else {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (colFamily != null && col != null) {
                del.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
            }else if (colFamily != null) {
                del.addFamily(Bytes.toBytes(colFamily));
            }else{
                System.out.println("nothing to delete");
            }

			/*
			 * 批量删除 List<Delete> deleteList = new ArrayList<Delete>(); deleteList.add(delete); table.delete(deleteList);
			 */
            table.delete(del);
            table.close();
            admin.close();
        }
    }

    /**
     * 根据RowKey获取数据
     *
     * @param tableName 表名称
     * @param rowKey RowKey名称
     * @param colFamily 列族名称
     * @param col 列名称
     * @throws IOException
     */
    public static Result getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
        }
        if (colFamily != null && col != null) {
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        Result result = table.get(get);
        //showCell(result);
        table.close();
        return result;
    }

    /**
     * 根据RowKey获取信息
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey) throws IOException {
        getData(tableName, rowKey, null, null);
    }

    /**
     * 格式化输出
     *
     * @param result
     */
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            println("rowkey: " + new String(CellUtil.cloneRow(cell)) + " ");
            println("Timetamp: " + cell.getTimestamp() + " ");
            println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
            println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
            println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
            println("--------------------------------");
        }
    }

    /**
     * 打印
     *
     * @param obj 打印对象
     */
    private static void println(Object obj) {
        System.out.println(obj);
    }
}
