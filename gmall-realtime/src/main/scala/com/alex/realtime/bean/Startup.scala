package com.alex.realtime.bean

case class Startup (mid:String,
               uid:String,
               appid:String,
               area:String,
               os:String,
               ch:String,
               logType:String,
               vs:String,
               var logDate:String,
               var logHour:String,
               var logHourMinute:String,
               var ts:Long
              ) {

}
