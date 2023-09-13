package document_operate.document_api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/14 14:28:07
 * @Description: GetDocument  查询数据
 * @Version 1.0.0
 */
public class GetDocument {
    public static void main(String[] args) throws IOException {

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        // 查询所有字段
        GetRequest getRequest = new GetRequest().index("idea_create").id("1");
        // 查询选择字段
        String[] includes = new String[]{"name", "age"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);

        // 连接查询数据
        GetResponse response = highLevelClient.get(getRequest, RequestOptions.DEFAULT);

        // 打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_type:" + response.getType());
        System.out.println("_id:" + response.getId());
        System.out.println("source:" + response.getSourceAsString());

//        // 转换成 Java Bean 对象
//        String jsonString = response.getSourceAsString();
//        ObjectMapper mapper = new ObjectMapper();
//        User user = mapper.readValue(jsonString, User.class);
//        System.out.println("当前数据 ==》" + user.toString());


        highLevelClient.close();

    }
}