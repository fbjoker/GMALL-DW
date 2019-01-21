package com.alex.gmall.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController
public class LogController {

    private static final  Logger logger = LoggerFactory.getLogger(LogController.class);

    @PostMapping("/log")
    public  String getLog(@RequestParam("log") String log){


        JSONObject jsonObject = JSON.parseObject(log);

        jsonObject.put("ts",System.currentTimeMillis());

        String jsonstr = jsonObject.toJSONString();

       // System.out.println(jsonstr);

       logger.info(jsonstr);


        return  null;


    }
}
