package com.alex.gmall.dw.gmallpublisher.service.impl;

import com.alex.gmall.dw.constant.GmallConstant;
import com.alex.gmall.dw.gmallpublisher.service.RealtimePublishService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class RealtimePublishServiceImpl implements RealtimePublishService {

    @Autowired
    JestClient jestClient;//Springboot带的连接ES的类,配置文件要写es的地址

    /**
     * 从ES里查询当日全部的日活
     * @param date
     * @return
     */
    @Override
    public int getTotal(String date) {

        int dauTotal=0;
        String query="{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \"2019-02-12\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        //可以直接用上面这种原生的方式,但是不够优雅
        // new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();


        //更优雅的做法
        //相当于原生ES里面的qurey
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //相当于原生里面的bool
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //相当于原生里面的term 里面写查询的字段和 过滤的条件
        TermQueryBuilder term = new TermQueryBuilder("logDate", date);
        boolQueryBuilder.filter(term);
        //把查询条件都加到searchbuild里面
        searchSourceBuilder.query(boolQueryBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();

        try {
            JestResult result = jestClient.execute(search);
            dauTotal= ((SearchResult) result).getTotal();//获取查询结果中的统计总数
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dauTotal;
    }


    /**
     *  从ES里查询当日的分时日活 ,前一天的日活
     * @param date
     * @return
     */
    @Override
    public Map getHour(String date) {
        Map hourMap= new HashMap();

        String query="GET gmall0808_dau/_search\n" +
                "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \"2019-02-13\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_hour\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"logHour\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        //分段的查询主要是加了一个聚合,之前的写法都一样
        //相当于原生ES里面的qurey
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        TermQueryBuilder term = new TermQueryBuilder("logDate", date);
        boolQueryBuilder.filter(term);

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合的写法
        TermsBuilder groupby_hour = AggregationBuilders.terms("groupby_hour");//给一个名字,取结果的时候要用到
        groupby_hour.field("logHour");

        //把聚合加到查询里
        searchSourceBuilder.aggregation(groupby_hour);
        //System.out.println(searchSourceBuilder.toString());


        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();

        try {
            SearchResult result = jestClient.execute(search);
            //从结果中取值,因为是term类型的聚合就用getTermsAggregation来取,最后加上getBuckets
            List<TermsAggregation.Entry> buckets = result.getAggregations().getTermsAggregation("groupby_hour").getBuckets();//使用刚才聚合使用的名称
            //遍历把结果加到map中
            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Long val = bucket.getCount();
                hourMap.put(key,val);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        return hourMap;
    }


    /**
     * 每日的交易总额
     * @param date
     * @return
     */
    @Override
    public  Double getTotalAmount(String date){
        Double totalAmount=0D;

        String query="GET /gmall0808_order/_search\n" +
                "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"createDate\": \"2019-02-14\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "\"aggs\": {\n" +
                "  \"total_sum\": {\n" +
                "    \"sum\": {        //聚合模式由terms改为sum,求和\n" +
                "      \"field\": \"totalAmount\"\n" +
                "    }\n" +
                "  }\n" +
                "}\n" +
                "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("createDate",date);
        boolQueryBuilder.filter(termQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);


        //聚合写法
        SumBuilder total_sum = AggregationBuilders.sum("total_sum");
        total_sum.field("totalAmount");//按那个来聚合
        //AggregationBuilders.sum("sum_totalAmount").field("totalAmount"); 另外一种写法

        searchSourceBuilder.aggregation(total_sum);


        Search build = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType("_doc").build();
        try {
            SearchResult result = jestClient.execute(build);

            totalAmount = result.getAggregations().getSumAggregation("total_sum").getSum();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return totalAmount;
    }

    @Override
    public Map getHourTotalAmount(String date) {
        Map hourTotalAmount=new HashMap();
        String query="GET /gmall0808_order/_search\n" +
                "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"createDate\": \"2019-02-14\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_hourdate\": {   // 先按照小时聚合\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"createHour\"  \n" +
                "      },\n" +
                "      \"aggs\": {\n" +
                "        \"sum_totalamount\": {   //然后小时按照amount求和\n" +
                "          \"sum\": {\n" +
                "            \"field\": \"totalAmount\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "      \n" +
                "    }\n" +
                "  }\n" +
                "  \n" +
                "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().filter(new TermQueryBuilder("createDate",date));

        searchSourceBuilder.query(boolQueryBuilder);

        //第一次聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_hourdate").field("createHour");

        //分时再聚合
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        //把第二次聚合加入第一次里面,相当于嵌套
        termsBuilder.subAggregation(sumBuilder);
        //把聚合加入
        searchSourceBuilder.aggregation(termsBuilder);
        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType("_doc").build();

        try {
            SearchResult result = jestClient.execute(search);

                //先取每个小时
            List<TermsAggregation.Entry> buckets = result.getAggregations().getTermsAggregation("groupby_hourdate").getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {

                String key = bucket.getKey();
                //再取第二次聚合的金额,这次直接取getSumAggregation就可以
                Double value = bucket.getSumAggregation("sum_totalamount").getSum();
                hourTotalAmount.put(key,value);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return hourTotalAmount;
    }


    /**
     * 从es中选择需求的数据
     * 要求的请求是这样的http://localhost:8080/sale_detail?date=2019-02-14&&startpage=1&&size=5&&keyword=手机双卡
     * 返回的json格式为一个total,一个stat里面有2个option,一个detail 数据的详细信息
     * {"total":62,
     * "stat":[{"options":[{"name":"20岁以下","value":0.0},{"name":"20岁到30岁","value":25.8},{"name":"30岁及30岁以上","value":74.2}],"title":"用户年龄占比"},
     *          {"options":[{"name":"男","value":38.7},{"name":"女","value":61.3}],"title":"用户性别占比"}],
     * "detail":[{"user_id":"9","sku_id":"8","user_gender":"M","user_age":49.0,"user_level":"1","order_price":8900.0,"sku_name":"Apple iPhone XS Max (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双
     */
@Override
    public Map getSaleDetail(String date , int startpage, int size,String keyword,String groupFiled){
        Map saleMap = new HashMap();
        //把这3个数据放在一个总的map里面返回
        long total=0;
        Map statMap = new HashMap();
        List detailList = new ArrayList();

        String query="GET gmall0808_sale/_search\n" +
                "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"dt\": \"2019-02-14\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [            //设置为必须模式,有才返回\n" +
                "        {\"match\":{            \n" +
                "          \"sku_name\": {\n" +
                "            \"query\": \"小米手机\",\n" +
                "            \"operator\": \"and\"    //and是匹配\"小米手机\"而不 配置 分词后的 \"小米\" \"手机\"\n" +
                "          }\n" +
                "         }\n" +
                "          \n" +
                "        }\n" +
                "     ]\n" +
                "    }\n" +
                "  }\n" +
                "  , \"aggs\":  {\n" +
                "    \"groupby_age\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_age\",\n" +
                "        \"size\":100               //当聚合种类超过10的数据必须要设置这个值,否则最后就只有10条数据\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  , \"from\": 0\n" +
                "  ,\"size\": 2        //ES的分页 从0行开始, 取2条,  如果要第N页的话,起始就是(N-1)*size\n" +
                "      \n" +
                "}";


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //filter过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().filter(new TermQueryBuilder("dt", date));

        if(keyword!=null&&keyword.length()>0){
        //设置must模式进行匹配,并且一定要选择and模式
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        }

        //聚合
        if(groupFiled!=null&&groupFiled.length()>0){
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby" + groupFiled).field(groupFiled);
        searchSourceBuilder.aggregation(termsBuilder);
        }

        //设置分页, 但凡是涉及到分页的一般都在后端做好
        searchSourceBuilder.from((startpage-1)*size);
        searchSourceBuilder.size(size);

        searchSourceBuilder.query(boolQueryBuilder);


        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);

            total=searchResult.getTotal();


            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby" + groupFiled).getBuckets();

            for (TermsAggregation.Entry bucket : buckets) {
                //把聚合后的统计结果保存到map
                statMap.put(bucket.getKey(),bucket.getCount());


            }
            //把数据放成一个对象最好,或者map也可以
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                //如果之前gethits传入的是一个对象那么这里取到的也是一个对象

                Map source = hit.source;
                //把匹配的对象放入list
                detailList.add(source);


            }

            saleMap.put("total",total);
            saleMap.put("stat",statMap);
            saleMap.put("detail",detailList);


        } catch (IOException e) {
            e.printStackTrace();
        }


        return saleMap;
    }
}



















