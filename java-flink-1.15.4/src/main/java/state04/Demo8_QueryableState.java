package state04;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @Auther: wxf
 * @Date: 2024/6/18 11:20:21
 * @Description: Demo8_QueryableState  状态查询
 * @Version 1.0.0
 */
public class Demo8_QueryableState {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        QueryableStateClient stateClient = new QueryableStateClient("localhost", 50400);

        ValueStateDescriptor<Tuple2<Long, Double>> descriptor = new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
        }), new Tuple2<Long, Double>(0L, 0.0D));

        JobID jobID = JobID.fromHexString("6c8efc6b251322c2b7230f6169687282");
        CompletableFuture<ValueState<Tuple2<Long, Double>>> future = stateClient.getKvState(jobID, "query_stat", 1, BasicTypeInfo.INT_TYPE_INFO, descriptor);

        Tuple2<Long, Double> value1 = future.get().value();
        System.out.println(value1);

//        future.thenAccept(response -> {
//            try {
//                Tuple2<Long, Double> value = response.value();
//                System.out.println(value);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });

    }
}