import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.transport.ElasticsearchTransport;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/11/7 15:32:39
 * @Description: IndexOperate
 * @Version 1.0.0
 */
public class IndexOperate {
    public static void main(String[] args) throws IOException {

        ElasticsearchTransport transport = CreateClient.getTransport();

        ElasticsearchClient client = new ElasticsearchClient(transport);
        ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);

        // 创建索引
        CreateIndexRequest request = new CreateIndexRequest.Builder().index("myindex").build();
        final CreateIndexResponse createIndexResponse = client.indices().create(request);
        System.out.println("创建索引成功：" + createIndexResponse.acknowledged());
        // 查询索引
        GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index("myindex").build();
        final GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest);
        System.out.println("索引查询成功：" + getIndexResponse.result());
        // 删除索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index("myindex").build();
        final DeleteIndexResponse delete = client.indices().delete(deleteIndexRequest);
        final boolean acknowledged = delete.acknowledged();
        System.out.println("删除索引成功：" + acknowledged);


        // 创建索引
        final Boolean acknowledged1 = client.indices().create(p -> p.index("")).acknowledged();
        System.out.println("创建索引成功");

        // 获取索引
        System.out.println(
                client.indices().get(
                        req -> req.index("myindex1")
                ).result());

        // 删除索引
        client.indices().delete( reqbuilder -> reqbuilder.index("myindex") ).acknowledged();


    }
}