package com.alex.gmall.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DemoController {


    @GetMapping("realtime-total")
    public  String realtimeTotal(@RequestParam("date") String date){




        return date+System.currentTimeMillis();
    }

}
