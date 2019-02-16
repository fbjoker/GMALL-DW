package com.alex.realtime.app

import com.alex.gmall.dw.constant.GmallConstant
import com.alex.gmall.util.{MyEsUtil, MyKafkaUtil}
import com.alex.realtime.bean.OrderInfo
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 消费kafka的数据并保存到ES中
  */
object OrderApp {
  def main(args: Array[String]): Unit = {

    


     val conf = new SparkConf().setAppName("orderapp").setMaster("local[*]")
      val sc = new SparkContext(conf)
       val ssc = new StreamingContext(sc,Seconds(5))

    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    val orderDstream: DStream[OrderInfo] = kafkaDstream.map(_.value()).map { orderstr =>

      val orderInfo: OrderInfo = JSON.parseObject(orderstr, classOf[OrderInfo])
      //对数据进行脱敏,必须不可逆
      orderInfo.consignee = orderInfo.consignee.splitAt(1)._1 + "***"
      orderInfo.consigneeTel = orderInfo.consigneeTel.splitAt(3)._1 + "***"

      //时间的处理  2019-01-30 08:44:15
      orderInfo.createDate = orderInfo.createTime.split(" ")(0)
      orderInfo.createHour = orderInfo.createTime.split(" ")(1).split(":")(0)
      orderInfo.createHourMinute = orderInfo.createTime.substring(11, 16)
     // println(orderInfo)

      orderInfo
    }

    //写入到ES中
    orderDstream.foreachRDD{rdd=>


      rdd.foreachPartition{ orderInfoItr=>
        //在ES里面按照id来索引
        MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_ORDER,orderInfoItr.toList,"id")

      }

    }

    ssc.start()
    ssc.awaitTermination()







  }

}
