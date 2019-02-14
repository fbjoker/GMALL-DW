package com.alex.gmall.dw.gmallpublisher.service;

import java.util.Map;

public interface RealtimePublishService {

    public int getTotal(String date);
    public Map getHour(String date);
}
