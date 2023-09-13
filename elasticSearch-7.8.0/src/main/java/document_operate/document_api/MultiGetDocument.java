package document_operate.document_api;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 17:20:47
 * @Description: MultiGetDocument 在单个http请求中并行执行多个get请求
 * @Version 1.0.0
 */
public class MultiGetDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        String index = "idea_create";

        MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.add(new MultiGetRequest.Item(index, "1"));
        multiGetRequest.add(new MultiGetRequest.Item(index, "5"));
        multiGetRequest.add(new MultiGetRequest.Item(index, "3"));

        MultiGetResponse responses = highLevelClient.mget(multiGetRequest, RequestOptions.DEFAULT);

        MultiGetItemResponse[] multiGetItemResponses = responses.getResponses();
        for (MultiGetItemResponse multiGetItemRespons : multiGetItemResponses) {
            String responsIndex = multiGetItemRespons.getIndex();
            String id = multiGetItemRespons.getId();
            GetResponse response = multiGetItemRespons.getResponse();
            if (response.isExists()) {
                String sourceAsString = response.getSourceAsString();
                System.out.println("index：" + responsIndex + " id：" + id + " source：" + sourceAsString);
            }
        }

        highLevelClient.close();
    }

}