package document_operate.document_api;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 16:51:36
 * @Description: ExistsDocument 判断是否存在数据
 * @Version 1.0.0
 */
public class ExistsDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        boolean exists = highLevelClient.exists(new GetRequest("idea_create", "2"), RequestOptions.DEFAULT);

        System.out.println("是否存在 index 为 2 的数据：" + exists);

        highLevelClient.close();

    }
}