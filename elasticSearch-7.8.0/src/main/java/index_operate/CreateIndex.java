package index_operate;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/13 17:00:34
 * @Description: CreateIndex  创建索引
 * @Version 1.0.0
 */
public class CreateIndex {
    public static void main(String[] args) throws IOException {

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        IndicesClient indicesClient = highLevelClient.indices();

        String index = "idea_create";
        if (indicesClient.exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
            indicesClient.delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
        }

        indicesClient.create(new CreateIndexRequest(index), RequestOptions.DEFAULT);

        boolean exists = indicesClient.exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
        System.out.println("是否存在 " + index + " ==> " + exists);

        highLevelClient.close();

    }
}