package document_operate.document_api;

import com.fasterxml.jackson.databind.ObjectMapper;
import document_operate.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 17:04:35
 * @Description: BulkDocument 批量操作
 * @Version 1.0.0
 */
public class BulkDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(clientBuilder);

        String index = "idea_create";
        ObjectMapper objectMapper = new ObjectMapper();

        // 创建 数据 -> 转换成 Json
        User user1 = new User("李白", 24, "男");
        User user2 = new User("杜甫", 23, "男");
        User user3 = new User("李商隐", 21, "男");
        User user4 = new User("李清照", 18, "女");
        String productJson1 = objectMapper.writeValueAsString(user1);
        String productJson2 = objectMapper.writeValueAsString(user2);
        String productJson3 = objectMapper.writeValueAsString(user3);
        String productJson4 = objectMapper.writeValueAsString(user4);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest().index(index).id("1").source(productJson1, XContentType.JSON));
        bulkRequest.add(new IndexRequest().index(index).id("2").source(productJson2, XContentType.JSON));
        bulkRequest.add(new IndexRequest().index(index).id("3").source(productJson3, XContentType.JSON));
        bulkRequest.add(new IndexRequest().index(index).id("4").source(productJson4, XContentType.JSON));

        BulkResponse response = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println("took:" + response.getTook());
        System.out.println("items:" + response.getItems());
        System.out.println("IngestTook:" + response.getIngestTook());

        highLevelClient.close();

    }
}