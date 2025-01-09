package state04;

import bean.Sensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: wxf
 * @Date: 2024/6/14 16:28:38
 * @Description: Demo6_Checkpointing  checkpoint配置案例
 * @Version 1.0.0
 */
public class Demo6_Checkpointing {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // 开启 CheckPoint 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000);
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms  注意这个值也意味着并发 checkpoint 的数目是一
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 允许两个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 开启实验性的 unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        SingleOutputStreamOperator<Sensor> socketDataStream = env.socketTextStream("localhost", 6666)
                .map(s -> {
                    String[] str = s.split(" ");
                    return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
                });

        socketDataStream.print();

        env.execute("Demo6_Checkpointing");

    }
}