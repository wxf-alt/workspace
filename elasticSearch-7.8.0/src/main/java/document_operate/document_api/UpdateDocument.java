package document_operate.document_api;

import com.fasterxml.jackson.databind.ObjectMapper;
import document_operate.User;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 14:43:59
 * @Description: UpdateDocument  更新数据
 * @Version 1.0.0
 */
public class UpdateDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        User user1 = new User("李白", 24, "男");
        ObjectMapper objectMapper = new ObjectMapper();
        String productJson = objectMapper.writeValueAsString(user1);

        UpdateRequest request = new UpdateRequest().index("idea_create").id("2")
                .doc(XContentType.JSON, "age", 22)
//                // 多字段更新
//                .doc(XContentType.JSON, "age", 22, "name", "王维")
                // 如果文档不存在/使用 upsert 插入 数据
                .upsert(productJson, XContentType.JSON);

        UpdateResponse response = highLevelClient.update(request, RequestOptions.DEFAULT);

        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());

        highLevelClient.close();

    }
}