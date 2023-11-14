import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.transport.ElasticsearchTransport;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/11/7 16:00:02
 * @Description: SearchOperate
 * @Version 1.0.0
 */
public class SearchOperate {

    public static void main(String[] args) throws IOException {
        ElasticsearchTransport transport = CreateClient.getTransport();

        ElasticsearchClient client = new ElasticsearchClient(transport);
        ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);

        final SearchRequest.Builder searchRequestBuilder = new SearchRequest.Builder().index("myindex1");
        MatchQuery matchQuery = new MatchQuery.Builder().field("city").query(FieldValue.of("beijing")).build();
        Query query = new Query.Builder().match(matchQuery).build();
        searchRequestBuilder.query(query);
        SearchRequest searchRequest = searchRequestBuilder.build();
        final SearchResponse<Object> search = client.search(searchRequest, Object.class);
        System.out.println(search);

        client.search(
                req -> {
                    req.query(
                            q ->
                                    q.match(
                                            m -> m.field("city").query("beijing")
                                    )
                    );
                    return req;
                }, Object.class
        );

    }
}