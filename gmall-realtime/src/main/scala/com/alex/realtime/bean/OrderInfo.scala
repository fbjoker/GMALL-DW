package com.alex.realtime.bean

case class OrderInfo(area: String,
                     var consignee: String,
                     orderComment: String,
                     var consigneeTel: String,

                     operateTime: String,
                     orderStatus: String,
                     paymentWay: String,
                     userId: String,
                     imgUrl: String,
                     totalAmount: Double,
                     expireTime: String,
                     deliveryAddress: String,
                     createTime: String,
                     trackingNo: String,
                     parentOrderId: String,
                     outTradeNo: String,
                     var id: String,
                     tradeBody: String,
                     var createDate: String,
                     var createHour: String,
                     var createHourMinute: String)
{   //需要显式的getset方法
    def getId():String ={
        id
    }
    def setId(id:String)={
      this.id=id
    }
}
