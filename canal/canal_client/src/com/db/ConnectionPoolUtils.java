package com.db;

import com.sync.common.GetProperties;

/**
 * Created by WangXiao on 2018/11/15.
 */
/*连接池工具类，返回唯一的一个数据库连接池对象,单例模式*/
public class ConnectionPoolUtils {
    private final static String driverName = "com.mysql.jdbc.Driver";
    private static String dbUrl = GetProperties.hbase_table_pk.getProperty("dbUrl");
    private static String dbUserName = GetProperties.hbase_table_pk.getProperty("dbUserName");
    private static String dbUserPwd = GetProperties.hbase_table_pk.getProperty("dbUserPwd");
    private ConnectionPoolUtils(){};//私有静态方法
    private static ConnectionPool poolInstance = null;
    public static ConnectionPool GetPoolInstance(){
        if(poolInstance == null) {
            poolInstance = new ConnectionPool(
                    driverName,
                    dbUrl,
                    dbUserName, dbUserPwd);
            try {
                poolInstance.createPool();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return poolInstance;
    }
}
