package com.alex.realtime.app

import java.text.SimpleDateFormat


import com.alex.gmall.dw.constant.GmallConstant
import com.alex.realtime.bean.Startup
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis


import java.util
import java.util.{Date, Properties}

import com.alex.gmall.util.{MyEsUtil, MyKafkaUtil, PropertiesUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object StatupApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("gmallrealtime").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))


    //使用工具类读取kafka中的数据
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    //把json字符串转为对象
    val startupDstream: DStream[Startup] = kafkaDstream.map(_.value()).map { startupjson =>
      val startup: Startup = JSON.parseObject(startupjson, classOf[Startup])
      startup
    }


    //从redis里面读取数据进行过滤
    //这里注意连接redis是在driver端,取出数据然后使用广播变量
    //transform 把一个时间段的当做一个RDD来处理
    val filterDstream: DStream[Startup] = startupDstream.transform { rdd =>

      //driver操作
      val properties: Properties = PropertiesUtil.load("config.properties")
      val jedisDriver = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)
      val dauSet: util.Set[String] = jedisDriver.smembers("dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      //把取出来的当日数据放到广播变量里面
      val dausetBC: Broadcast[util.Set[String]] = sc.broadcast(dauSet)


      jedisDriver.close()
      println("过滤前:" + rdd.count())

      //这个在excutor处理
      val rddFilter: RDD[Startup] = rdd.filter { startup =>

        !dausetBC.value.contains(startup.mid) //当在redis里的时候就过滤掉,(满足条件就留下)
      }

      println("过滤后:" + rddFilter.count())
      rddFilter
    }

    //把过滤后的数据写入到redis
    filterDstream.foreachRDD{rdd=>
      //不能把连接建立在这里是在driver里,excutor获取不到


      rdd.foreachPartition{ startupIter=>
        //应该吧连接建立在这里,excutor里
        val properties: Properties = PropertiesUtil.load("config.properties")
        val jedisExcutor = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)


        val list = new ListBuffer[Startup]()

        for (startup <- startupIter) {

          //把时间加上
          startup.logDate=new SimpleDateFormat("yyyy-MM-dd").format(new Date(startup.ts))
          startup.logHour=new SimpleDateFormat("HH").format(new Date(startup.ts))
          startup.logHourMinute=new SimpleDateFormat("HH:mm").format(new Date(startup.ts))
          //把过后的结果写到redis中
          jedisExcutor.sadd("dau:"+startup.logDate,startup.mid)

          list+=startup
        }
        jedisExcutor.close()

        //批量把过滤后结果写入到ES中
        MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_DAU,list.toList,null);


      }


    }

    ssc.start()
    ssc.awaitTermination()


  }

}
