package com.alex.gamll.canel.client;

import com.alex.gamll.canel.handler.CanalHandler;
import com.alex.gamll.canel.util.MyKafkaProducer;
import com.alex.gmall.dw.constant.GmallConstant;
import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.CaseFormat;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalClient {
    public static void main(String[] args) {

        //获取canel的连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while(true){

            canalConnector.connect();//这个连接是瞬时的所以要用while(true)来保持连接
            canalConnector.subscribe("gmall0808.order_info");//指定要监控的库表
            Message message = canalConnector.get(100);//抓取数据,message是很多条SQL执行的结果集

            if(message.getEntries().size()==0 ){
             //如果数据为空则说明目前服务器空闲等待一段时间
                try {
                    Thread.sleep(5000);
                    System.out.println("没有数据休息5S");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
            //处理业务
            List<CanalEntry.Entry> entries = message.getEntries();  //把message转换为entry集合,一个entry集合为一条sql的结果集
                //遍历entry集合得到每条entry


                for (CanalEntry.Entry entry : entries) {
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //排除掉类型是开启和断开的事务,剩下的就是需要的数据
                    if(entryType== CanalEntry.EntryType.TRANSACTIONBEGIN&&entryType== CanalEntry.EntryType.TRANSACTIONEND){
                        continue;
                    }

                   //把entry数据反序列化为RowChange

                    CanalEntry.RowChange rowChange=null;
                    try {
                         rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                    //获取行数据集并遍历行数据
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                    //获取表名
                    String tableName = entry.getHeader().getTableName();
                    //把业务抽取成方法
                    CanalHandler.handle(rowChange, rowDatasList, tableName);


                }


            }



        }

    }


}
