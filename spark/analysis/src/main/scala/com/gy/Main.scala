package com.gy

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON
import org.apache.hadoop.security.UserGroupInformation
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
  * Created by Administrator on 2020/4/7 0007.
  */
object Main {
  case class spm_dara(uid:String,channel:String,browser:String,mobile:String,resource_spm:String,spm_value:String,action:String,spm_time:String,other:String)
  def main(args:Array[String]):Unit={
    ///new KerberosAuth().kerberos(false)
    val conf = new SparkConf().setAppName(ConfigFactory.sparkstreamname)
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", args(0))
    conf.set("spark.default.parallelism", "30")
    conf.set("spark.streaming.backpressure.enabled","true")
    conf.set("hive.metastore.authorization.storage.checks","false")
    conf.set("hive.security.authorization.enabled","false")
    conf.set("spark.sql.hive.caseSensitiveInferenceMode","INFER_ONLY")
    //conf.set("spark.streaming.backpressure.initialRate","2") 只在reciever机制下起作用
    conf.set("spark.yarn.stagingDir","hdfs:///user/spark_works")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(ConfigFactory.sparkstreamseconds))
    ssc.checkpoint(ConfigFactory.checkpointdir+getDate())
    val topics = Array(ConfigFactory.kafkatopic)
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
      import spark.implicits._
      val data = rdd.mapPartitions(par=>{
        par.map(x=>{
          val value = x.value()
          val json = JSON.parseObject(value)
          val uid= if(json.containsKey("uid")) json.get("uid").toString else ""
          val channel= if(json.containsKey("channel")) json.get("channel").toString else ""
          val browser= if(json.containsKey("browser")) json.get("browser").toString else ""
          val mobile=if(json.containsKey("mobile")) json.get("mobile").toString else ""
          val resource_spm= if(json.containsKey("resource_spm")) json.get("resource_spm").toString else ""
          val spm_value = if(json.containsKey("spm_value")) json.get("spm_value").toString else ""
          val action=if(json.containsKey("action")) json.get("action").toString else ""
          val spm_time=if(json.containsKey("spm_time")) json.get("spm_time").toString else ""
          val other=if(json.containsKey("other")) json.get("other").toString else ""
          spm_dara(uid,channel,browser,mobile,resource_spm,spm_value,action,spm_time,other)
        })
      })

      data.toDF().createOrReplaceTempView("table1") //将DataSet格式数据映射到临时表中
      data.toDF().show()
      val sql="insert into dyg_ods.spm_data partition(static_day='"+getDate()+"') select uid,channel,browser,mobile,resource_spm,spm_value,action," +
        "spm_time,other from table1"
      spark.sql(sql)//在hive上运行sql语句将临时表中数据抽出并存入hive中
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }



  def getDate():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    var now=dateFormat.format(cal.getTime())
    now
  }
}
