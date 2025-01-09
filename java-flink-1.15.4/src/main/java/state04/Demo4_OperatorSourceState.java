package state04;

import bean.Sensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: wxf
 * @Date: 2024/6/14 13:54:26
 * @Description: Demo4_OperatorSourceState  算子状态 下面的例子中的 SourceFunction 在 CheckpointedFunction 中进行数据缓存，程序意外终止,利用CheckPoint恢复运行,断点续传
 * @Version 1.0.0
 */
public class Demo4_OperatorSourceState {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        // TODO 本地测试需要添加参数 表示从 CheckPoint 恢复任务
//         file:///E:\A_data\3.code\workspace\java-flink-1.15.4\src\main\resources\Demo4_OperatorSourceState\e1fb57403faf7ba468d21b85022eab62\chk-49
        conf.setString("execution.savepoint.path", args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // 设置 CheckPoint
        env.enableCheckpointing(3000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("file:///E:\\A_data\\3.code\\workspace\\java-flink-1.15.4\\src\\main\\resources\\Demo4_OperatorSourceState");
        checkpointConfig.setCheckpointTimeout(3000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<Sensor> sensorDataStreamSource = env.addSource(new CounterSource());
        sensorDataStreamSource.print();

        env.execute("Demo4_OperatorSourceState");

    }

    static class CounterSource extends RichParallelSourceFunction<Sensor> implements CheckpointedFunction {

        private List<Sensor> sensorList = new ArrayList<>();

        // flag for job cancellation
        private volatile boolean isRunning = true;

        // 存储 state 的变量.
        private ListState<Sensor> listState;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            for (Sensor sensor : sensorList) {
                listState.add(sensor);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Sensor> sensorListStateDescriptor = new ListStateDescriptor<>("sensor-state", Sensor.class);
            listState = context.getOperatorStateStore().getListState(sensorListStateDescriptor);

            if (context.isRestored()) {
                for (Sensor sensor1 : listState.get()) {
                    sensorList.add(sensor1);
                }
            }
        }

        @Override
        public void run(SourceContext<Sensor> ctx) throws Exception {
            while (isRunning) {
                if (sensorList.size() == 5) {
                    for (Sensor sensor : sensorList) {
                        Thread.sleep(1000);
                        ctx.collect(sensor);
                    }
                    sensorList.clear();
                }
                Sensor e = new Sensor(0, "offset_" + System.currentTimeMillis(), 1.2D);
                System.out.println("e --> " + e);
                sensorList.add(e);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

    }

}