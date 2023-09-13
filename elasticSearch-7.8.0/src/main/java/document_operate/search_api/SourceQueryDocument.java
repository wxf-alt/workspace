package document_operate.search_api;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/15 11:37:01
 * @Description: SourceQueryDocument 抓取相关字段
 * @Version 1.0.0
 */
public class SourceQueryDocument {
    public static void main(String[] args) throws IOException {
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        String[] includeFields = new String[] {"name", "age"};
        String[] excludeFields = new String[] {};
        sourceBuilder.fetchSource(includeFields,excludeFields);

        SearchRequest searchRequest = new SearchRequest("idea_create");
        searchRequest.source(sourceBuilder);

        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println("Took：" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());

        SearchHits hits = response.getHits();
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        for (SearchHit hit : hits) {
            System.out.println(hit.getId() + " ==》" + hit.getSourceAsString());
        }

        highLevelClient.close();
    }
}