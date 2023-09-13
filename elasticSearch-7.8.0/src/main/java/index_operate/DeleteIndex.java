package index_operate;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/13 17:00:34
 * @Description: DeleteIndex  删除索引
 * @Version 1.0.0
 */
public class DeleteIndex {
    public static void main(String[] args) throws IOException {

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        IndicesClient indicesClient = highLevelClient.indices();

        AcknowledgedResponse response = indicesClient.delete(new DeleteIndexRequest("idea_create"), RequestOptions.DEFAULT);

        System.out.println("操作结果 ： " + response.isAcknowledged());

        highLevelClient.close();

    }
}