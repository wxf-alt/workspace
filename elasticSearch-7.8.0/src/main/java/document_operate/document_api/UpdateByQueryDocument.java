package document_operate.document_api;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.Collections;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 19:54:18
 * @Description: UpdateByQueryDocument 根据查询请求执行更新。  姓李的用户 age+1
 * @Version 1.0.0
 */
public class UpdateByQueryDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices("idea_create", "idea_create_copy");
        updateByQueryRequest.setConflicts("proceed"); //设置版本冲突时继续
        updateByQueryRequest.setQuery(new TermQueryBuilder("name", "李"));
        updateByQueryRequest.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "ctx._source.age++;",
                        Collections.emptyMap()));
//        updateByQueryRequest.setMaxDocs(2);
//        updateByQueryRequest.setBatchSize(100);

        BulkByScrollResponse response = highLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        System.out.println("Took：" + response.getTook());
        System.out.println("Updated：" + response.getUpdated());

        highLevelClient.close();

    }
}