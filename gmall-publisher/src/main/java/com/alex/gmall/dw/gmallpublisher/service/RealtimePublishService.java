package com.alex.gmall.dw.gmallpublisher.service;

import java.util.Map;

public interface RealtimePublishService {

    public int getTotal(String date);
    public Map getHour(String date);

    public  Double getTotalAmount(String date);
    public  Map getHourTotalAmount(String date);

    public Map getSaleDetail(String date , int startpage, int size,String keyword,String groupFiled);
}
