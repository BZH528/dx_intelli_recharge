package com.gy.hcharge

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}




/**
  * Created by Administrator on 2019/11/14 0014.
  */
object Mysql {

  def writeToMysql(menu:String, feature_name:String, feature_value:String, time:String)={
    var conn: Connection = null//定义mysql连接
    var ps: PreparedStatement = null
    val sql = "insert into " + ConfigFactory.database +"."+ConfigFactory.realTable +"(menu,feature_name,feature_value,date,time) values (?,?,?,?,?)"//需要执行的sql语句，两个 “？”代表后面需要替换的数据
    try{
      val str = ConfigFactory.mysqlurl+"/"+ConfigFactory.database+"?&useUnicode=true&characterEncoding=utf8"
      conn = DriverManager.getConnection(str,ConfigFactory.mysqlusername, ConfigFactory.mysqlpassword)
      ps = conn.prepareStatement(sql)
      ps.setString(1, menu)//将需要写入mysql的数据进行映射
      ps.setString(2, feature_name)
      ps.setString(3, feature_value)
      ps.setString(4,time.split(" ")(0))
      ps.setString(5, time)
      ps.executeUpdate()//在mysql上执行sql语句将数据插入到相应的表中
    }catch {
        case e: Exception => println("Mysql Exception")
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()//关闭mysql连接
        }
      }
  }


//  def flushData(spark:SparkSession): Unit ={
//    val str = "jdbc:mysql://"+ConfigFactory.mysqlurl+"/"+ConfigFactory.database+
//      "?user="+ConfigFactory.mysqlusername+"&password="+ConfigFactory.mysqlpassword
//    val sql = "(select * from "+ConfigFactory.database +"."+ConfigFactory.realTable+" where time=max(time)) as aaa"
//    val df = spark.read
//      .format("jdbc")
//      .options(Map("url" ->
//        //配置mysql连接参数，包括mysql ip 端口  数据库名称 登录名和密码
//        str,
//        //定义驱动程序
//        "driver"->"com.mysql.jdbc.Driver",
//        //编写sql  在mysql中执行该sql并返回数据
//        "dbtable" -> sql))
//      .load()
//    val url = "jdbc:mysql://"+ConfigFactory.mysqlurl+"/"+ConfigFactory.database
//    df.write.mode(SaveMode.Append).format("jdbc")
//      .option("url", url)//定义mysql 地址 端口 数据库
//      .option("dbtable", ConfigFactory.dayTable)//定义需要插入的mysql目标表
//      .option("user", ConfigFactory.mysqlusername)//定义登录用户名
//      .option("password", ConfigFactory.mysqlpassword)//定义登录密码
//      .save()//保存数据
//  }

}
