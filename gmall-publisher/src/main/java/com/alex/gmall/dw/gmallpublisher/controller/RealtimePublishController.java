package com.alex.gmall.dw.gmallpublisher.controller;

import com.alex.gmall.dw.gmallpublisher.bean.Option;
import com.alex.gmall.dw.gmallpublisher.bean.Stat;
import com.alex.gmall.dw.gmallpublisher.service.impl.RealtimePublishServiceImpl;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
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
        //当日总活跃用户
        Map totalMap=new HashMap();
        int total = realtimePublishService.getTotal(date);
        totalMap.put("id","dau");
        totalMap.put("name","活跃用户");
        totalMap. put("value",total);


        totalList.add(totalMap);

        //当日交易总额
        Map totalAmountMap=new HashMap();
        Double totalAmount = realtimePublishService.getTotalAmount(date);
        totalAmountMap.put("id","order_total_amount");
        totalAmountMap.put("name","交易总额");
        totalAmountMap. put("value",totalAmount);


        totalList.add(totalAmountMap);



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
        }else if("order_total_amount".equals(id)){
            System.out.println("======================>");

            Map  orderTotalAmountInfoMap=new HashMap();
            Map orderTotalAmountTodayMap = realtimePublishService.getHourTotalAmount(date);
            orderTotalAmountInfoMap.put("today",orderTotalAmountTodayMap);
            Date yesterday=null;
            try {
                yesterday = DateUtils.addDays(new SimpleDateFormat("yyyy-MM-dd").parse(date), -1);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Map orderTotalAmountYesterdayMap = realtimePublishService.getHourTotalAmount(new SimpleDateFormat("yyyy-MM-dd").format(yesterday));
            orderTotalAmountInfoMap.put("yesterday",orderTotalAmountYesterdayMap);

            return JSON.toJSONString(orderTotalAmountInfoMap);

        }

        return  "";
    }


    /**
     * 要求的请求是这样的http://localhost:8080/sale_detail?date=2019-02-14&&startpage=1&&size=5&&keyword=手机双卡
     * 这里使用HttpServletRequest 来获取参数,  因为用RequestParam的方法如果缺少参数就会报错
     *
     *返回的数据格式
     * {"total":62,
     *  "stat":[{"options":[{"name":"20岁以下","value":0.0},{"name":"20岁到30岁","value":25.8},{"name":"30岁及30岁以上","value":74.2}],"title":"用户年龄占比"},
     *           {"options":[{"name":"男","value":38.7},{"name":"女","value":61.3}],"title":"用户性别占比"}],
     *  "detail":[{"user_id":"9","sku_id":"8","user_gender":"M","user_age":49.0,"user_level":"1","order_price":8900.0,"sku_name":"Apple iPhone XS Max (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双
     *
     *
     */
    @GetMapping("sale_detail")
    public String getSaleDetail(HttpServletRequest request){

        String date = request.getParameter("date");
        int startpage = Integer.parseInt( request.getParameter("startpage"));
        int size = Integer.parseInt( request.getParameter("size"));
        String keyword = request.getParameter("keyword");


        Map saleageDateMap = realtimePublishService.getSaleDetail(date, startpage, size, keyword, "user_age");
        //取的数据只是每个年龄的统计,并不是年龄段, 还需要再加工
        Map<String,Long> ageMap= (Map<String,Long>)saleageDateMap.get("stat");

        //设置每个年龄段
        Long ageLt20=0L;
        Long age20to30=0L;
        Long ageGt30=0L;

        for (Map.Entry<String, Long> entry : ageMap.entrySet()) {
            int key = Integer.parseInt(entry.getKey());
            if (key < 20) {
                ageLt20 += entry.getValue();
            } else if (key >= 20 && key < 30) {
                age20to30 += entry.getValue();
            } else {
                ageGt30 += entry.getValue();
            }
        }

        long total = (Long) saleageDateMap.get("total");

        Double ageLt20Rate= Math.round (1000d*ageLt20/total)/10d;
        Double age20to30Rate= Math.round (1000d*age20to30/total)/10d;
        Double ageGt30Rate= Math.round (1000d*ageGt30/total)/10d;

        List<Stat> statList=new ArrayList<>();


        //加入第一个option  年龄比例
        List<Option> ageOptions=new ArrayList();
        ageOptions.add(new Option("20岁以下",ageLt20Rate));
        ageOptions.add(new Option("20岁到30岁",age20to30Rate));
        ageOptions.add(new Option("30岁及30岁以上",ageGt30Rate));
        Stat ageStat = new Stat("用户年龄占比", ageOptions);
        statList.add(ageStat);

        //计算男女比例
        Map salegenderDateMap = realtimePublishService.getSaleDetail(date, startpage, size, keyword, "user_gender");
        Map<String,Long> genderMap= (Map<String,Long>)salegenderDateMap.get("stat");

        //这里是不是可以直接取出来不用遍历??
        long maleCount=0L;
        long femaleCount=0L;

        for (Map.Entry<String, Long> genderEntry : genderMap.entrySet()) {
            String gender = genderEntry.getKey() ;
            Long genderCount = genderEntry.getValue();
            if(gender.equals("M")){
                maleCount=genderCount;
            } else {
                femaleCount=genderCount;
            }
        }

        double maleRatio= Math.round(1000.0D*maleCount/total)/10D ;
        double femaleRatio=Math.round(1000.0D*femaleCount/total)/10D;

        //加入第二个option 性别比例
        List<Option> genderOptions=new ArrayList();
        genderOptions.add(new Option("男",maleRatio));
        genderOptions.add(new Option("女",femaleRatio));

        Stat genderStat = new Stat("用户性别占比", genderOptions);
        statList.add(genderStat);

        Map saleMap = new HashMap<>();
        saleMap.put("detail",salegenderDateMap.get("detail"));
        saleMap.put("total",total);
        saleMap.put("stat",statList);

        return  null;

    }

}
