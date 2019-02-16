package com.alex.gmall.export2es.app

import com.alex.gmall.dw.constant.GmallConstant
import com.alex.gmall.export2es.bean.SaleDetailDaycount
import com.alex.gmall.util.MyEsUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 从hive里的宽表读取数据导入到ES中,
  * 需要使用sparksql和es的工具类
  * 并且需要hive-site.xml和log4j的配置在resources里
  * 需要用到mysql的依赖
  */
object Export2EsApp {

  def main(args: Array[String]): Unit = {
    var dt="";

    if(args!=null&&args.length>0){
      dt=args(0)
    }else{
       dt="2019-02-15"
    }


    val conf: SparkConf = new SparkConf().setAppName("hive2es").setMaster("local[*]")
    //sparksql不需要sc因为session里就有, 而且获得session后就直接导入隐式转换
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._

    //直接将dateset转换为rdd,注意在sql里面吧数据类型改为跟样例类一样的
      val rdd: RDD[SaleDetailDaycount] = sparkSession.sql("select user_id," +
        "sku_id,user_gender," +
        "cast(user_age as int) user_age," +
        "user_level,cast(order_price as double)," +
        "sku_name,sku_tm_id," +
        "sku_category3_id," +
        "sku_category2_id," +
        "sku_category1_id," +
        "sku_category3_name," +
        "sku_category2_name," +
        "sku_category1_name," +
        "spu_id,sku_num," +
        "cast(order_count as bigint) order_count," +
        "cast(order_amount as double) order_amount," +
        "dt from gmall0808.dws_sale_detail_daycount where dt='" + dt +"'" ).as[SaleDetailDaycount].rdd

      rdd.foreachPartition{saleIter=>
        //注意这里不能直接使用saleIter.tolist因为saleIter有可能很大,这样就直接撑爆了,需要分解为N条一传,具体条数根据业务调整
        var i =0;
         val saleDataillist= new ListBuffer[SaleDetailDaycount]()
        for (sale <- saleIter) {
          i+=1
          saleDataillist += sale
          //10条一传,具体根据业务调整
          if(i%10==0){

            MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_SALE,saleDataillist.toList,null)
            //情况列表,重新放入数据
            saleDataillist.clear()
          }



        }
        //不够10整除的最后剩余的数据再最后发一波
        if(saleDataillist.size>0){
          MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_SALE,saleDataillist.toList,null)
        }





      }


  }


}
