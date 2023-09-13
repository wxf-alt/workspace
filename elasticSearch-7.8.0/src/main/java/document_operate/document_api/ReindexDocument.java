package document_operate.document_api;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 19:39:23
 * @Description: ReindexDocument 用于将文档从一个或多个索引复制到目标索引中。
 * @Version 1.0.0
 */
public class ReindexDocument {
    public static void main(String[] args) throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("idea_create");
        reindexRequest.setDestIndex("idea_create_copy");
        reindexRequest.setMaxDocs(3);
        reindexRequest.setSourceBatchSize(100);

        BulkByScrollResponse response = highLevelClient.reindex(reindexRequest, RequestOptions.DEFAULT);

        System.out.println("took:" + response.getTook());
        System.out.println("create：" + response.getCreated());
        System.out.println(response.toString());

        highLevelClient.close();

    }
}