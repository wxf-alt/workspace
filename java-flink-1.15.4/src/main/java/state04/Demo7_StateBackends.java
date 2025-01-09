package state04;

import bean.Sensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: wxf
 * @Date: 2024/6/18 09:40:28
 * @Description: Demo7_StateBackends  状态后端-案例
 *                  Flink 内置了以下这些开箱即用的 state backends ：
 *                  HashMapStateBackend
 *                  EmbeddedRocksDBStateBackend
 *               如果不设置，默认使用 HashMapStateBackend。
 * @Version 1.0.0
 */
public class Demo7_StateBackends {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // 设置 StateBackend - jobmanager
        env.setStateBackend(new HashMapStateBackend());

        // 设置 StateBackend - rocksdb  true-开启增量快照
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        // 通过编程方式为每个作业开启或关闭 Changelog  减少 CheckPoint 时间
        env.enableChangelogStateBackend(true);

        // 旧版本的 MemoryStateBackend 等价于使用 HashMapStateBackend 和 JobManagerCheckpointStorage。
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // 旧版本的 FsStateBackend 等价于使用 HashMapStateBackend 和 FileSystemCheckpointStorage。
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));

        // 旧版本的 RocksDBStateBackend 等价于使用 EmbeddedRocksDBStateBackend 和 FileSystemCheckpointStorage.
        env.setStateBackend(new RocksDBStateBackend(""));
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));


        SingleOutputStreamOperator<Sensor> socketDataStream = env.socketTextStream("localhost", 6666)
                .map(s -> {
                    String[] str = s.split(" ");
                    return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
                });

        socketDataStream.print();

        env.execute("Demo7_StateBackends");

    }
}