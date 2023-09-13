package document_operate.document_api;

import com.fasterxml.jackson.databind.ObjectMapper;
import document_operate.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 11:03:10
 * @Description: PutDocument  添加数据
 * @Version 1.0.0
 */
public class PutDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(clientBuilder);

        // 创建 数据 -> 转换成 Json
        User user1 = new User("李白", 24, "男");
//        User user2 = new User("杜甫", 23, "男");
//        User user3 = new User("李商隐", 21, "男");
//        User user4 = new User("李清照", 18, "女");
        ObjectMapper objectMapper = new ObjectMapper();
        String productJson = objectMapper.writeValueAsString(user1); // 对象序列化 -> Json
        User jsonUser = objectMapper.readValue(productJson, User.class); // Json字符串反序列化 -> 对象

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("idea_create"); // 设置索引
        indexRequest.id("1"); // 设置 主键
        indexRequest.source(productJson, XContentType.JSON); // 添加 数据

        // 添加数据
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        ////3.打印结果信息
        System.out.println("_index:" + indexResponse.getIndex());
        System.out.println("_id:" + indexResponse.getId());
        System.out.println("_result:" + indexResponse.getResult());
        RestStatus status = indexResponse.status();
        System.out.println("name-toString：" + status.toString());
        System.out.println("name：" + status.name());
        System.out.println("getStatus：" + status.getStatus());

        restHighLevelClient.close();

    }
}