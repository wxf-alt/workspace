package state04;

import bean.Sensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: wxf
 * @Date: 2024/6/12 10:07:37
 * @Description: Demo3_OperatorSinkState  算子状态 下面的例子中的 SinkFunction 在 CheckpointedFunction 中进行数据缓存，然后统一发送到下游，这个例子演示了列表状态数据的 event-split redistribution。
 * @Version 1.0.0
 */
public class Demo3_OperatorSinkState {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        // TODO 本地测试需要添加参数 表示从 CheckPoint 恢复任务
        // file:///E:\A_data\3.code\workspace\java-flink-1.15.4\src\main\resources\Demo3_OperatorSinkState\e1fb57403faf7ba468d21b85022eab62\chk-49
        conf.setString("execution.savepoint.path", args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // 设置 CheckPoint
        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("file:///E:\\A_data\\3.code\\workspace\\java-flink-1.15.4\\src\\main\\resources\\Demo3_OperatorSinkState");
        checkpointConfig.setCheckpointTimeout(5000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> socketDataStream = env.socketTextStream("localhost", 6666);

        SingleOutputStreamOperator<Sensor> mapDataStream = socketDataStream.map(s -> {
            String[] str = s.split(" ");
            return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
        });

        mapDataStream.addSink(new BufferingSink(5));

        env.execute("Demo3_OperatorState");
    }

    static class BufferingSink implements SinkFunction<Sensor>, CheckpointedFunction {

        private final int threshold;

        private transient ListState<Sensor> checkpointedState;

        private List<Sensor> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Sensor value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() >= threshold) {
                for (Sensor element : bufferedElements) {
                    // 打印-输出
                    System.out.println(element);
                }
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (Sensor element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            ListStateDescriptor listStateDescriptor = new ListStateDescriptor("buffered-elements", TypeInformation.of(new TypeHint<ListState<Sensor>>() {
            }));
            checkpointedState = context.getOperatorStateStore().getListState(listStateDescriptor);

            if (context.isRestored()) {
                for (Sensor sensor : checkpointedState.get()) {
                    bufferedElements.add(sensor);
                }
            }

        }

    }

}