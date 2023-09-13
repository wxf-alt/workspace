package index_operate;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/13 16:52:17
 * @Description: CreateClient
 * @Version 1.0.0
 */
public class CreateClient {
    public static void main(String[] args) throws IOException {

//         默认是 http -> 可以省略最后一个参 数
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);


        highLevelClient.close();
    }
}