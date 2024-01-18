import java.util.Random

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.streampark.flink.connector.kafka.bean.KafkaRecord
import org.apache.streampark.flink.connector.kafka.source.KafkaSource
import org.apache.streampark.flink.core.scala.{FlinkStreaming, StreamingContext}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.streampark.common.util.JsonUtils
import org.apache.streampark.flink.connector.jdbc.sink.JdbcSink
import org.apache.streampark.flink.core.StreamEnvConfig

/**
 * @Auther: wxf
 * @Date: 2024/1/17 10:56:10
 * @Description: QuickStartApp
 * @Version 1.0.0
 */
object QuickStartApp extends FlinkStreaming {

  override def handle(): Unit = {
    //    val source: DataStream[KafkaRecord[String]] = KafkaSource().getDataStream[String]()

    val source: DataStream[User] = context.addSource(new MyDataSource)

    source.print()

    JdbcSink().sink[User](source)(user =>
      s"""
         |insert into t_user(`name`,`age`,`gender`,`address`)
         |value('${user.name}',${user.age},${user.gender},'${user.address}')
         |""".stripMargin
    )
    context.start()

  }

  case class User(name: String, age: Int, gender: Int, address: String)

}

