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
import java.io.Serializable;

/**
 * @Auther: wxf
 * @Date: 2023/9/15 10:52:21
 * @Description: TermQueryDocument 字段匹配 值查询
 * @Version 1.0.0
 */
public class TermQueryDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        // 设置 查询
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("name", "李"));
        sourceBuilder.from(0);
        sourceBuilder.size(2);

        // 设置 search 索引
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("idea_create");
        searchRequest.source(sourceBuilder);

        SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        System.out.println("Took：" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());

        SearchHits hits = response.getHits();
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        highLevelClient.close();

    }
}