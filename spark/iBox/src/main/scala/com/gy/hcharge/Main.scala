package com.gy.hcharge

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Created by Administrator on 2019/12/6 0006.
  */
object Main {
  def main(args:Array[String]):Unit={
    startJob("1000")
  }

  def startJob(consumeRate:String): Unit ={
    // 初始化配置文件

    val conf = new SparkConf().setAppName(ConfigFactory.sparkstreamname)
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", consumeRate)
    conf.set("spark.default.parallelism", "30")
    conf.set("spark.yarn.stagingDir","hdfs:///user/spark_works")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


    while (true) {
      val ssc = StreamingContext.getOrCreate(ConfigFactory.checkpointdir+Util.getDate(), getStreamingContext _)
      ssc.start()
      ssc.awaitTerminationOrTimeout(Util.resetTime)
      ssc.stop(false, true)
    }

    def getStreamingContext(): StreamingContext = {
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
        //"enable.auto.commit" -> (false: java.lang.Boolean),
        "auto.offset.reset" -> "latest"
      )


      //读取kafka数据流
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      stream.filter(item=>{
        if(item.topic().equals(topics(0))){
          false
        }else{
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
        }
      }).map(x=>{
        val json = JSON.parseObject(x.value())
        val data = json.getJSONObject("after")
        val money = data.getOrDefault("money_amount","0.0").toString.toDouble
        ("order",(1L,money))
      }).mapWithState(StateSpec.function(orderMapFunction)).stateSnapshots()
        .foreachRDD(rdd=>{
          val now = Util.getFormatTime
          rdd.foreach(x=>{
            System.out.println(x._1 + ":"+ x._2._1+":"+x._2._2)
            Mysql.writeToMysql(x._1,"order_count", String.valueOf(x._2._1),now)
            Mysql.writeToMysql(x._1,"order_account", String.valueOf(x._2._2),now)
          })
        })



      //提取数据流中的uid和chanal属性
      val uid_chanal_rdd = stream.filter(item=>{
        val tmp =item.topic().equals(topics(0))
        val json = JSON.parseObject(item.value())
        if(json.containsKey("uid")&& json.containsKey("channel")&&tmp){
          true
        }else{
          false
        }
      }).map(x=>{
        val json = JSON.parseObject(x.value())
        (json.get("uid").toString, json.get("channel").toString)
      }).cache()



      uid_chanal_rdd.reduceByKey((a,b)=>a).mapPartitions(par=>{
        //////////////////////////////////////////////
        ////this maybe use more memery
        /////////////////////////////////////////////
        val userMap =new scala.collection.mutable.HashMap[String,String]
        par.foreach(x=>{
          if(!userMap.contains(x._1)){
            userMap += (x._1->x._2)
          }
        })
        Hbase.getNewUserList(userMap,ConfigFactory.all_user).toIterator
      }).mapWithState(StateSpec.function(mapFunction)).stateSnapshots().foreachRDD(rdd=>{
        val now = Util.getFormatTime
        rdd.collect().foreach(x=>{
          val str = "newAddCustomer,"+x._1+","+String.valueOf(x._2)
          Mysql.writeToMysql("newAddCustomer",x._1, String.valueOf(x._2),now)
        })
      })

      //将数据以UID为rowkey写入hbase进行去重
      //@param "user" 用来存储当天访问慧充值小程序的uid
      uid_chanal_rdd.foreachRDD(rdd=>{
        //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //获取offset
        val will_write_rdd = rdd.map(x=>{
          val family = Bytes.toBytes(ConfigFactory.family)
          val column = Bytes.toBytes(ConfigFactory.column)
          var put = new Put(Bytes.toBytes(x._1))
          put.addImmutable(family, column, Bytes.toBytes(x._2))
          (new ImmutableBytesWritable, put)
        }).cache()
        will_write_rdd.saveAsNewAPIHadoopDataset(Hbase.getKerberosConfiguration("dx_intelli_recharge@BIGDATA1.COM"
          ,ConfigFactory.confPath +"dx_intelli_recharge.keytab",ConfigFactory.all_user))
        val visit_user_count=Hbase.getRowCount(ConfigFactory.all_user)
        val str = "visit,visit_count,"+String.valueOf(visit_user_count)
        val now = Util.getFormatTime
        Mysql.writeToMysql("visit","visit_count",String.valueOf(visit_user_count), now)
        //stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) //存储offset
      })
      ssc
    }
  }


  // 实时流量状态更新函数
  val mapFunction = (datehour: String, pv: Option[Long], state: State[Long]) => {
    val accuSum = pv.getOrElse(0L) + state.getOption().getOrElse(0L)
    val output = (datehour, accuSum)
    state.update(accuSum)
    output
  }

  val orderMapFunction = (datehour: String, pv: Option[(Long,Double)], state: State[(Long,Double)]) => {
    val pv_tmp = pv.getOrElse((0L,0.0))
    val state_tmp = state.getOption().getOrElse((0L, 0.0))
    val order_count = pv_tmp._1 + state_tmp._1
    val order_money = pv_tmp._2 + state_tmp._2
    val output = (datehour,(order_count, order_money))
    state.update((order_count, order_money))
    output
  }

}


