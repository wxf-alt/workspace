import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._

/**
 * @Auther: wxf
 * @Date: 2024/1/17 14:50:00
 * @Description: FlinkWeiUiDemo
 * @Version 1.0.0
 */
object FlinkWeiUiDemo {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.setInteger(RestOptions.PORT, 8081)
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment // 不会开启WebUI
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val socketInputStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    socketInputStream.print()

    env.execute()
  }
}
