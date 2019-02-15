package com.alex.gamll.canel.handler;

import com.alex.gamll.canel.util.MyKafkaProducer;
import com.alex.gmall.dw.constant.GmallConstant;
import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.CaseFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalHandler {

    public static void handle(CanalEntry.RowChange rowChange, List<CanalEntry.RowData> rowDatasList, String tableName) {
        //判断结果是否是insert产生并且是指定表中的数据
        if(rowChange.getEventType()==CanalEntry.EventType.INSERT&&"order_info".equals(tableName)){

            //遍历每行获取每列
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                //遍历每列把sql的表名改为对应类的属性(驼峰命名)
                Map orderMap= new HashMap();
                for (CanalEntry.Column column : afterColumnsList) {
                    String filed = column.getName();
                    String value = column.getValue();
                    //使用工具改为驼峰命名
                    String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, filed);

                    orderMap.put(key,value);


                }
                //发送到kafka中

                MyKafkaProducer.send(GmallConstant.KAFKA_TOPIC_ORDER, JSON.toJSONString(orderMap));

            }
        }
    }
}
