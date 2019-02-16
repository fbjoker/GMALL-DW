package com.alex.gmall.gmalllogger.controller;

import com.alex.gmall.dw.constant.GmallConstant;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Random;

@RestController
public class LogController {

    private static final  Logger logger = LoggerFactory.getLogger(LogController.class);

    @Autowired
    KafkaTemplate kafkaTemplate;

    @PostMapping("/log")
    public  String getLog(@RequestParam("log") String log){


        JSONObject jsonObject = JSON.parseObject(log);


        jsonObject.put("ts",System.currentTimeMillis());

        String jsonstr = jsonObject.toJSONString();

       // System.out.println(jsonstr);

       logger.info(jsonstr);


        return  null;


    }

    @PostMapping("/logrealtime")
    public  String getLogrealtime(@RequestParam("log") String log){


        JSONObject jsonObject = JSON.parseObject(log);


//        jsonObject.put("ts",System.currentTimeMillis()+3600*1000*24 +new Random().nextInt(3600*1000*5));
        jsonObject.put("ts",System.currentTimeMillis()+new Random().nextInt(3600*1000*5));

        String jsonstr = jsonObject.toJSONString();

        if("startup".equals(jsonObject.getString("type"))){

            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonstr);
        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonstr);

        }

        logger.info(jsonstr);


        return  null;


    }
}
