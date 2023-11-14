import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.transport.ElasticsearchTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: wxf
 * @Date: 2023/11/7 15:52:24
 * @Description: DocumentOperate
 * @Version 1.0.0
 */
public class DocumentOperate {
    public static void main(String[] args) throws IOException {

        ElasticsearchTransport transport = CreateClient.getTransport();
        ElasticsearchClient client = new ElasticsearchClient(transport);

        User user = new User(1, "z3", 22);

        // 创建文档
        IndexRequest indexRequest = new IndexRequest.Builder()
                .index("myindex")
                .id(user.getId().toString())
                .document(user)
                .build();
        final IndexResponse index = client.index(indexRequest);
        System.out.println("文档操作结果:" + index.result());

        // 批量创建文档
        final List<BulkOperation> operations = new ArrayList<BulkOperation>();
        for (int i = 1; i <= 5; i++) {
            final CreateOperation.Builder builder = new CreateOperation.Builder();
            builder.index("myindex");
            builder.id("200" + i);
            builder.document(new User(2000 + i, "zhangsan" + i, i + 15));
            final CreateOperation<Object> objectCreateOperation = builder.build();
            final BulkOperation bulk = new
                    BulkOperation.Builder().create(objectCreateOperation).build();
            operations.add(bulk);
        }
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).build();
        final BulkResponse bulkResponse = client.bulk(bulkRequest);
        System.out.println("数据操作成功：" + bulkResponse);

        // 删除文档
        DeleteRequest deleteRequest = new DeleteRequest.Builder().index("myindex").id("1001").build();
        client.delete(deleteRequest);


        // 创建文档
        System.out.println(
                client.index(
                        req ->
                                req.index("myindex")
                                        .id(user.getId().toString())
                                        .document(user)
                ).result()
        );

        // 批量创建文档
        ArrayList<User> users = new ArrayList<>();
        users.add(new User(1,"z3",21));
        users.add(new User(1,"l4",23));
        users.add(new User(1,"w5",20));
        client.bulk(
                req -> {
                    users.forEach(
                            u -> {
                                req.operations(
                                        b -> {
                                            b.create(
                                                    d ->
                                                            d.id(u.getId().toString()).index("myindex").document(u)
                                            );
                                            return b;
                                        }
                                );
                            }
                    );
                    return req;
                }
        );

        // 删除文档
        client.delete(
                req -> req.index("myindex").id("1001")
        );


    }
}