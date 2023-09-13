package document_operate.search_api;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/15 11:02:33
 * @Description: MatchQueryDocument 模糊查询
 * @Version 1.0.0
 */
public class MatchFuzzinessQueryDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        MatchQueryBuilder queryBuilder = new MatchQueryBuilder("name", "李");
//        sourceBuilder.query(QueryBuilders.matchQuery("name", "李").fuzziness(Fuzziness.AUTO).prefixLength(1).maxExpansions(3));
        sourceBuilder.query(QueryBuilders.fuzzyQuery("name","李").fuzziness(Fuzziness.ONE));

        SearchRequest searchRequest = new SearchRequest("idea_create");
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