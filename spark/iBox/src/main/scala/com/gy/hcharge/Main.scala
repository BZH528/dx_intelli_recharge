package com.gy.hcharge

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Created by Administrator on 2019/12/6 0006.
  */
object Main {
  def main(args:Array[String]):Unit={
    startJob(args(0))
  }

  def startJob(consumeRate:String): Unit ={
    // 初始化配置文件

    val conf = new SparkConf().setAppName(ConfigFactory.sparkstreamname)
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", consumeRate)
    conf.set("spark.default.parallelism", "30")
    conf.set("spark.streaming.backpressure.enabled","true")
    //conf.set("spark.streaming.backpressure.initialRate","2") 只在reciever机制下起作用
    conf.set("spark.yarn.stagingDir","hdfs:///user/spark_works")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    while (true) {
      val ssc = StreamingContext.getOrCreate(ConfigFactory.checkpointdir+Util.getDate(), getStreamingContext _)
      Thread.sleep(300000)
      ssc.start()
      ssc.awaitTerminationOrTimeout(Util.resetTime)
      ssc.stop(false, true)
    }

    def getStreamingContext(): StreamingContext = {

      val tmpTableName = "tmp:cacheTable_"+Util.getDate()
      val hbaseAdmin = HbaseUtils.admin
      try{
        if(!hbaseAdmin.tableExists(TableName.valueOf(tmpTableName))){

          HbaseUtils.createNewTable(tmpTableName)
        }
      }catch {
        case e:Exception=>e.printStackTrace()
      }

      //HbaseUtils.truncateTable(ConfigFactory.today_visit_user)
      //Mysql.flushData(spark)
      val ssc = new StreamingContext(sc, Seconds(ConfigFactory.sparkstreamseconds))
      ssc.checkpoint(ConfigFactory.checkpointdir+Util.getDate())
      val topics = ConfigFactory.kafkatopic
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> ConfigFactory.kafkaipport,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "security.protocol" -> "SASL_PLAINTEXT",
        "sasl.kerberos.service.name" -> "kafka",
        "group.id" -> ConfigFactory.kafkagroupid,
        "enable.auto.commit" -> (false: java.lang.Boolean),
        "auto.offset.reset" -> "latest"
      )


      //读取kafka数据流
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )


      stream.foreachRDD(rdd=>{
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.persist(StorageLevel.MEMORY_AND_DISK)
        rdd
          .filter(item=>orderFilterFunction(item,topics(1)))
          .map(x=>{
            val json = JSON.parseObject(x.value())
            val data = json.getJSONObject("after")
            val money = data.getOrDefault("money_amount","0.0").toString.toDouble
            ("order",(1L,money))
          })
          .reduceByKey((a,b)=> (a._1+b._1, a._2+b._2))
          .collect()
          .foreach(x=>orderStatisticUpdateAndWriteToMysql(x))

        val uidChanalRDD = rdd
          .filter(item=>visitFilterFunction(item,topics(0)))
          .map(x=>{
              val json = JSON.parseObject(x.value())
              (json.get("uid").toString, json.get("channel").toString)
          }).cache()

          val batch_count = uidChanalRDD.count()
          clickCountUpdateAndWriteToMysql(batch_count)

          uidChanalRDD.reduceByKey((a,b)=>a)
          .mapPartitions(par=>{
            val userMap =new scala.collection.mutable.HashMap[String,String]
            par.foreach(x=>{
              if(!userMap.contains(x._1)){
                userMap += (x._1->x._2)
              }
            })
            Hbase.getNewUserList(userMap,ConfigFactory.all_user).toIterator
          })
          .reduceByKey(_+_)
          .collect()
          .foreach(x=>newUserUpdateAndWriteToMysql(x))


        uidChanalRDD
          .filter(r=> r._1 != "" && r._1!=null&&r._1.length>0)
          .map(x=>{
            val family = Bytes.toBytes(ConfigFactory.family)
            val column = Bytes.toBytes(ConfigFactory.column)
            var put = new Put(Bytes.toBytes(x._1))
            put.addImmutable(family, column, Bytes.toBytes(x._2))
            (new ImmutableBytesWritable, put)
        }).saveAsNewAPIHadoopDataset(Hbase.getKerberosConfiguration("dx_intelli_recharge@BIGDATA1.COM"
          ,ConfigFactory.confPath +"dx_intelli_recharge.keytab",ConfigFactory.all_user))

        val visit_user_count=Hbase.getRowCount(ConfigFactory.all_user)
        val str = "visit,visit_count,"+String.valueOf(visit_user_count)
        val now = Util.getFormatTime
        Mysql.writeToMysql("visit","visit_count",String.valueOf(visit_user_count), now)
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })

      ssc
    }
  }

  def orderFilterFunction(item:ConsumerRecord[String,String],topic:String):Boolean={
    if(item.topic().equals(topic)){
      val json = JSON.parseObject(item.value())
      val head = json.getJSONObject("head")
      val process_type = head.get("type").toString
      val after = json.getJSONObject("after")
      if(process_type.equals("INSERT")){
        val suc = after.get("order_status").toString.toLowerCase.equals("order_success") || after.get("order_status").toString.toLowerCase.equals("pay_success")
        suc
      }
      else if(process_type.equals("UPDATE")){
        val before= json.getJSONObject("before")
        if(!before.get("order_status").toString.toLowerCase().equals("pay_success") && after.get("order_status").toString.toLowerCase().equals("pay_success")){
          true
        }else{
          false
        }
      }else{
        false
      }
    }else{
      false
    }
  }


  def visitFilterFunction(item:ConsumerRecord[String,String],topic:String):Boolean={
    val tmp =item.topic().equals(topic)
    val json = JSON.parseObject(item.value())
    if(json.containsKey("uid")&& json.containsKey("channel")&&tmp){
      true
    }else{
      false
    }
  }

  def orderStatisticUpdateAndWriteToMysql(x:(String,(Long,Double)))={
    val now = Util.getFormatTime
    //val old_order_count = Hbase.getValue("tmp:cacheTable_"+Util.getDate(),"order_count")
    val old_order_count = Hbase.getValue("tmp:cacheTable_"+Util.getDate(),"order_count")
    var result_order_count = 0L
    if(!old_order_count.equals("")){
      result_order_count = old_order_count.toLong
    }
    val new_order_count = result_order_count+x._2._1
    Hbase.putValue("tmp:cacheTable_"+Util.getDate(),"order_count",new_order_count.toString)
    Mysql.writeToMysql(x._1,"order_count", String.valueOf(new_order_count),now)

    val old_order_account = Hbase.getValue("tmp:cacheTable_"+Util.getDate(),"order_account")
    var result_order_account = 0.0
    if(!old_order_account.equals("")){
      result_order_account = old_order_account.toDouble
    }
    val new_order_account = result_order_account+x._2._2
    Hbase.putValue("tmp:cacheTable_"+Util.getDate(),"order_account",new_order_account.toString)
    Mysql.writeToMysql(x._1,"order_account", String.valueOf(new_order_account),now)
  }

  def newUserUpdateAndWriteToMysql(x:(String,Long))={
    val now = Util.getFormatTime
    val old_count = Hbase.getValue("tmp:cacheTable_"+Util.getDate(),x._1)
    var result_count = 0L
    if(!old_count.equals("")){
      result_count = old_count.toLong
    }
    val new_count = result_count+x._2
    Hbase.putValue("tmp:cacheTable_"+Util.getDate(),x._1,new_count.toString)
    val str = "newAddCustomer,"+x._1+","+String.valueOf(new_count)
    Mysql.writeToMysql("newAddCustomer",x._1, String.valueOf(new_count),now)
  }

  def clickCountUpdateAndWriteToMysql(count:Long)={
    val now = Util.getFormatTime
    val old_count = Hbase.getValue("tmp:cacheTable_"+Util.getDate(),"click_count")
    var result_count = 0L
    if(!old_count.equals("")){
      result_count = old_count.toLong
    }
    val new_count = result_count+count
    Hbase.putValue("tmp:cacheTable_"+Util.getDate(),"click_count",new_count.toString)
    Mysql.writeToMysql("visit","click_count", String.valueOf(new_count),now)
  }

}


