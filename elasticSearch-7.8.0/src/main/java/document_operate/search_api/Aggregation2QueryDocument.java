package document_operate.search_api;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/15 15:06:52
 * @Description: AggregationQueryDocument 分组聚合查询
 * @Version 1.0.0
 */
public class Aggregation2QueryDocument {
    public static void main(String[] args) throws IOException, InterruptedException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.terms("sex_group").field("sex.keyword")
                        .subAggregation(AggregationBuilders.avg("age_avg").field("age"))
                        .subAggregation(AggregationBuilders.stats("stats_age").field("age"))
        );

        SearchRequest searchRequest = new SearchRequest("idea_create");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

//        //4.打印响应结果
//        System.out.println(response);

//        Terms ageGroup = response.getAggregations().get("age_group");
//        for (Terms.Bucket bucket : ageGroup.getBuckets()) {
//            Avg ageAvg = bucket.getAggregations().get("age_avg");
//            System.out.println(bucket.getKey() + " ==》" + ageAvg.getValue());
//        }

        //4.打印响应结果
        System.out.println(response);

        Terms ageGroup = response.getAggregations().get("sex_group");
        for (Terms.Bucket bucket : ageGroup.getBuckets()) {
            Aggregations aggregations = bucket.getAggregations();
            Avg ageAvg = aggregations.get("age_avg");
            System.out.println(bucket.getKey() + " age_avg ==》" + ageAvg.getValue());
            Stats statsAge = aggregations.get("stats_age");
            System.out.println(bucket.getKey() + " count ==》" + statsAge.getCount());
            System.out.println(bucket.getKey() + " avg ==》" + statsAge.getAvgAsString());
            System.out.println(bucket.getKey() + " max ==》" + statsAge.getMaxAsString());
            System.out.println(bucket.getKey() + " min ==》" + statsAge.getMinAsString());
            System.out.println(bucket.getKey() + " sum ==》" + statsAge.getSumAsString());
        }

        highLevelClient.close();

    }
}