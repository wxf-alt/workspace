package document_operate.document_api;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 15:52:04
 * @Description: DeleteDocument 删除数据
 * @Version 1.0.0
 */
public class DeleteDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        DeleteRequest deleteRequest = new DeleteRequest();
        deleteRequest.index("idea_create").id("2");

        DeleteResponse response = highLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(response.toString());
        System.out.println(response.status().getStatus());
        System.out.println(response.getResult().getLowercase());
        System.out.println(response.getResult().getOp());

        highLevelClient.close();

    }
}