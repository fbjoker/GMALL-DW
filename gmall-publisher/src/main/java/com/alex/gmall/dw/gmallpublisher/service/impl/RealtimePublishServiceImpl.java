package com.alex.gmall.dw.gmallpublisher.service.impl;

import com.alex.gmall.dw.constant.GmallConstant;
import com.alex.gmall.dw.gmallpublisher.service.RealtimePublishService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
}
