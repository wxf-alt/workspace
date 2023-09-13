package document_operate.document_api;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 20:21:42
 * @Description: DeleteByQueryDocument 按查询请求执行删除。
 * @Version 1.0.0
 */
public class DeleteByQueryDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
        deleteByQueryRequest.indices("idea_create_copy");
        deleteByQueryRequest.setQuery(new MatchQueryBuilder("name", "李"));
        deleteByQueryRequest.setMaxDocs(1);

        BulkByScrollResponse response = highLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);

        System.out.println("Took：" + response.getTook());
        System.out.println("Deleted：" + response.getDeleted());


        highLevelClient.close();

    }
}