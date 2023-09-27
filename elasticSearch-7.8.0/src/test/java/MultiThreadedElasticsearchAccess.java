import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * @Auther: wxf
 * @Date: 2023/9/21 15:08:06
 * @Description: MultiThreadedElasticsearchAccess
 * @Version 1.0.0
 */
public class MultiThreadedElasticsearchAccess {
    private static final String INDEX_NAME = "my_index";
    private static final String DOC_TYPE = "_doc";
    private static final String DOC_ID = "my_doc_id";
    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;

    public static void main(String[] args) {
        // 创建 ES 客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ES_HOST, ES_PORT, "http"))
        );

        // 创建多个线程并启动
        int numThreads = 5;
        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(new WorkerThread(client));
            thread.start();
        }
    }

    // 工作线程，每个线程尝试更新文档
    private static class WorkerThread implements Runnable {
        private final RestHighLevelClient client;

        public WorkerThread(RestHighLevelClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            try {
                // 模拟多线程并发操作
                for (int i = 0; i < 10; i++) {
                    // 读取文档，获取当前版本
                    GetResponse getResponse = client.get(
                            new GetRequest(INDEX_NAME, DOC_ID),
                            RequestOptions.DEFAULT
                    );

                    // 解析当前版本
                    long currentVersion = getResponse.getVersion();

                    // 构建新的文档内容
                    String updatedDocument = "{\"name\": \"UpdatedName\"}";

                    // 尝试更新文档（指定版本号）
                    IndexRequest indexRequest = new IndexRequest(INDEX_NAME, DOC_TYPE, DOC_ID)
                            .source(updatedDocument, XContentType.JSON)
                            .version(currentVersion);  // 指定当前版本号

                    try {
                        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED
                                || indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                            // 更新成功
                            System.out.println("Thread " + Thread.currentThread().getId() + ": Document updated successfully");
                        }
                    } catch (ElasticsearchException e) {
                        if (e.status() == RestStatus.CONFLICT) {
                            // 处理版本冲突
                            System.out.println("Thread " + Thread.currentThread().getId() + ": Version conflict, handle it");
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
