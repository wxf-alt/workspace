import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;

/**
 * @Auther: wxf
 * @Date: 2023/11/7 16:10:04
 * @Description: AsyncClientOperate
 * @Version 1.0.0
 */
public class AsyncClientOperate {
    public static void main(String[] args) {

        ElasticsearchTransport transport = CreateClient.getTransport();

        ElasticsearchAsyncClient asyncClient = new ElasticsearchAsyncClient(transport);

        // 创建索引
        asyncClient.indices().create(
                req -> {
                    req.index("newindex");
                    return req;
                }
        ).whenComplete(
                (resp, error) -> {
                    System.out.println("回调函数");
                    if (resp != null) {
                        System.out.println(resp.acknowledged());
                    } else {
                        error.printStackTrace();
                    }
                }
        );
        System.out.println("主线程操作...");
        asyncClient.indices().create(
                req -> {
                    req.index("newindex");
                    return req;
                }
        )
                .thenApply(
                        resp -> {
                            return resp.acknowledged();
                        }
                )
                .whenComplete(
                        (resp, error) -> {
                            System.out.println("回调函数");
                            if (!resp) {
                                System.out.println();
                            } else {
                                error.printStackTrace();
                            }
                        }
                );

    }
}