package com.alex.gmall.dw.gmallpublisher.controller;

import com.alex.gmall.dw.gmallpublisher.service.impl.RealtimePublishServiceImpl;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class RealtimePublishController {

    @Autowired
    RealtimePublishServiceImpl realtimePublishService;


    /**
     * 要求返回的json串格式[{"id":"dau","name":"新增日活","value":1200},{"id":"new_mid","name":"新增用户","value":233}]
     * @param date
     * @return
     */
    @GetMapping("realtime-total")
    public  String realtimeTotal(@RequestParam("date") String date){
        List totalList= new ArrayList<Map>();
        Map totalMap=new HashMap();
        int total = realtimePublishService.getTotal(date);
        totalMap.put("id","dau");
        totalMap.put("name","活跃用户");
        totalMap. put("value",total);


        totalList.add(totalMap);

        return JSON.toJSONString(totalList);
    }

    /**
     * 要求的数据格式{"yesterday":{"11":383,"12":123,"17":88,"19":200 },"today":{"12":38,"13":1233,"17":123,"19":688 }}
     * @param date
     * @return
     */
    @GetMapping("realtime-hour")
    public  String realtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){

        if("dau".equals(id)){
            Map hourMap= new HashMap();

            //当天的数据
            Map todayHour = realtimePublishService.getHour(date);
            hourMap.put("today",todayHour);

            //昨日的数据
            Date yesterday=null;
            try {
                yesterday = DateUtils.addDays(new SimpleDateFormat("yyyy-MM-dd").parse(date), -1);
            } catch (ParseException e) {
                e.printStackTrace();
            }
                String yesterdayStr = new SimpleDateFormat("yyyy-MM-dd").format(yesterday);

            Map yesterdayHour = realtimePublishService.getHour(yesterdayStr);

            hourMap.put("yesterday",yesterdayHour);

            return JSON.toJSONString(hourMap);
        }



        return  "";
    }

}
