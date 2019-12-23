package com.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class ConnectionTest {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        try {
                  /*使用连接池创建100个连接的时间*/
                   /*// 创建数据库连接库对象
                   ConnectionPool connPool = new ConnectionPool("com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/test", "root", "123456");
                   // 新建数据库连接库
                   connPool.createPool();*/

            ConnectionPool connPool = ConnectionPoolUtils.GetPoolInstance();//单例模式创建连接池对象
            // SQL测试语句
            String summary_date = "2018-12-12 10";
            String client_id = "1001";
            String sql = "select mcd_count from activity_track_today_hour where source=\'" + "hbase_1001" + "\'"  + "and statis_hour=\'" + summary_date + "\'";
            String sql_insert = String.format("insert into activity_track_today_hour(source,record_status,statis_hour,mcd_count) " +
                            "values('%s',0,'%s',%d)",
                    "hbase_1001",summary_date,
                    107);

            System.out.println(sql);
            // 设定程序运行起始时间
            long start = System.currentTimeMillis();
            // 循环测试100次数据库连接
            //for (int i = 0; i < 100; i++) {
                Connection conn = connPool.getConnection(); // 从连接库中获取一个可用的连接
                Statement stmt = conn.createStatement();
                int currentDateRecordNum = 0;
                ResultSet rs = stmt.executeQuery(sql);

                if (rs.next()) {
                    currentDateRecordNum = rs.getInt("mcd_count");
                    System.out.println("mcd_count===" + currentDateRecordNum);
                }
                rs.close();
                if (currentDateRecordNum == 0){
                    //insert
                }else{
                    //update
                }
            System.out.println("------------------");
            stmt.executeUpdate(sql_insert);
                stmt.close();
                connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
            //}
            System.out.println("经过100次的循环调用，使用连接池花费的时间:" + (System.currentTimeMillis() - start) + "ms");
            // connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
            connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}