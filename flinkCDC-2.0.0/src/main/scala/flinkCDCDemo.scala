import com.ververica.cdc.connectors.mysql.MySqlSource
import com.ververica.cdc.connectors.mysql.table.StartupOptions
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}

/**
 * @Auther: wxf
 * @Date: 2023/8/21 10:35:07
 * @Description: flinkCDCDemo
 * @Version 1.0.0
 */
object flinkCDCDemo {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    // file:///E:\A_data\4.测试数据\MySqlFlinkCDC
    //    conf.setString("execution.savepoint.path", args(0)) // 读取本地 checkpoint
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE)
    val checkpointConfig: CheckpointConfig = env.getCheckpointConfig
    // 确认 checkpoints 之间的最小时间 500 ms  检查点之间的默认最小暂停:none。
    checkpointConfig.setMinPauseBetweenCheckpoints(500L)
    // Checkpoint 必须在一分钟内完成，否则就会被抛弃（超时时间） 尝试检查点的默认超时时间:10分钟。
    checkpointConfig.setCheckpointTimeout(60000L)
    // 允许两个连续的 checkpoint 错误  默认值为0，这意味着不容忍检查点失败，并且作业将在第一次报告检查点失败时失败。
    checkpointConfig.setTolerableCheckpointFailureNumber(2)
    // 同一时间只允许一个 checkpoint 进行   并发发生检查点的默认限制:1
    checkpointConfig.setMaxConcurrentCheckpoints(1)
    // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
    checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 开启实验性的 unaligned checkpoints 未对齐检查点
    checkpointConfig.enableUnalignedCheckpoints()

    // 相当于 RocksDBStateBackend
    env.setStateBackend(new FsStateBackend("file:///E:\\A_data\\4.测试数据\\MySqlFlinkCDC"))

    // mysql-cdc 创建
    val mySqlSource = MySqlSource
      .builder()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("root")
      .databaseList("db")
      .tableList("db.area_copy")
      .startupOptions(StartupOptions.initial())
      .deserializer(new StringDebeziumDeserializationSchema())
      .build()

    env.addSource(mySqlSource)
      .print().setParallelism(1)

    env.execute("MySqlFlinkCDC")

  }
}
