package index_operate;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @Auther: wxf
 * @Date: 2023/9/13 17:38:43
 * @Description: GetIndex 获取索引信息
 * @Version 1.0.0
 */
public class GetIndex {
    public static void main(String[] args) throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        GetIndexResponse response = highLevelClient.indices().get(new GetIndexRequest("idea_create"), RequestOptions.DEFAULT);
        System.out.println("aliases:" + response.getAliases());
        System.out.println("mappings:" + response.getMappings());
        System.out.println("settings:" + response.getSettings());

        Map<String, MappingMetadata> mappings = response.getMappings();
        MappingMetadata indexMapping = mappings.get("idea_create");
        Map<String, Object> sourceAsMap = indexMapping.getSourceAsMap();
        Set<String> keySet = sourceAsMap.keySet();
        for (String s1 : keySet) {
            Object o = sourceAsMap.get(s1);
            System.out.println("s1：" + s1 + " | o：" + o);
        }

        highLevelClient.close();

    }
}