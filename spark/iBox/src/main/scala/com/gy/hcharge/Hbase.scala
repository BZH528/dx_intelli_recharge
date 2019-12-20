package com.gy.hcharge

import java.io.File
import java.security.PrivilegedAction
import java.util

import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.{AggregationClient, LongColumnInterpreter}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation



/**
  * Created by Administrator on 2019/11/15 0015.
  */
object Hbase {



  def getRowCount(table:String):Long={
    var count=0L
    var admin:Admin =null
    var aggregationClient:AggregationClient=null
    try{
      admin = HbaseUtils.getHbaseAdmin
      val name:TableName = TableName.valueOf(table)
      val descriptor:HTableDescriptor = admin.getTableDescriptor(name)
      val coprocessorClass:String = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation"
      if(!descriptor.hasCoprocessor(coprocessorClass)){
        admin.disableTable(name);
        descriptor.addCoprocessor(coprocessorClass)
        admin.modifyTable(name, descriptor)
        admin.enableTable(name)
      }
      val scan:Scan = new Scan()
      scan.setTimeRange(NumberUtils.toLong(Util.getDayStartTime()), NumberUtils.toLong(Util.getDayEndTime()))
      aggregationClient = HbaseUtils.getAggregationClient
      count = aggregationClient.rowCount(name, new LongColumnInterpreter(), scan)
    }catch{
      case e:Exception =>e.printStackTrace()
    }
    return  count
  }


  def getNewUserList(userMap:scala.collection.mutable.HashMap[String,String] , tableName:String)={
    var userList:List[String] = List()
    var getList: List[Get] = List()
    var reList:List[(String,Long)] = List()
    val table:Table = HbaseUtils.getTable(TableName.valueOf(tableName))
    try{
      for(entry<-userMap){
        userList = userList:+entry._1
        getList = getList:+new Get(Bytes.toBytes(entry._1))
      }
      import scala.collection.JavaConverters._
      val results:Array[Result] = table.get(getList.asJava)
      for(i<-0 until results.length) {
        if (results(i).isEmpty) {
          val rowkey:String = userList(i)
          reList = reList:+(userMap.get(rowkey).get,1L)
        }
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }

    reList

  }


  def getHBaseConn( principal: String, keytabPath: String): Connection = {
    System.setProperty("java.security.krb5.conf", ConfigFactory.confPath +"krb5.conf")
    val configuration = HBaseConfiguration.create
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/core-site.xml"))
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/hdfs-site.xml"))
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/hbase-site.xml"))
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    val loginUser = UserGroupInformation.getLoginUser
    loginUser.doAs(new PrivilegedAction[Connection] {
      override def run(): Connection = ConnectionFactory.createConnection(configuration)
    })
  }


  def getKerberosConfiguration(principal: String, keytabPath: String,table:String)= {
    val configuration = HBaseConfiguration.create
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/core-site.xml"))
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/hdfs-site.xml"))
    configuration.addResource(new Path(ConfigFactory.confPath +"hbase-conf/hbase-site.xml"))
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)
    val loginUser = UserGroupInformation.getLoginUser
    loginUser.doAs(new PrivilegedAction[Configuration] {
      override def run()={
        configuration.set(TableOutputFormat.OUTPUT_TABLE, table)
        val job = Job.getInstance(configuration)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.getConfiguration
      }
    })
  }

}
