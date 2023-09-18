package connector

import connector.FlinkSupportElasticSearch.User
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * @Auther: wxf
 * @Date: 2023/9/18 16:05:05
 * @Description: FlinkSqlSupportElasticSearch  flink写入Es
 * @Version 1.0.0
 */
object FlinkSqlSupportElasticSearch {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    //    conf.set(RestOptions.BIND_PORT, "8089")
    conf.setString(RestOptions.BIND_PORT, "8081-8089")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)

    val environmentSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, environmentSettings)

    val dataStream: DataStream[String] = env.socketTextStream("localhost", 6666)
    val splitDataStream: DataStream[(Int, String, String, String)] = dataStream.map(x => {
      val str: Array[String] = x.split(",")
      (str(0).toInt, str(1), str(2), str(3))
    })
    val table: Table = tableEnv.fromDataStream(splitDataStream, 'id, 'name, 'sex, 'ts)
    tableEnv.createTemporaryView("datagen", table)

    //    // 生成器
    //    tableEnv.executeSql(
    //      """CREATE TABLE datagen (
    //        | id INT,
    //        | name STRING,
    //        | sex STRING,
    //        | ts AS localtimestamp,
    //        | WATERMARK FOR ts AS ts
    //        |) WITH (
    //        | 'connector' = 'datagen',
    //        | 'rows-per-second'='2',
    //        | 'fields.id.kind'='sequence',
    //        | 'fields.id.start'='1',
    //        | 'fields.id.end'='5',
    //        | 'fields.name.length'='2',
    //        | 'fields.sex.length'='1'
    //        |)
    //        |""".stripMargin)

    //    es 连接器
    tableEnv.executeSql(
      """
        |CREATE TABLE esSinkTable (
        | id INT,
        | name STRING,
        | sex STRING,
        | ts STRING,
        | PRIMARY KEY (id) NOT ENFORCED
        |) WITH (
        |  'connector' = 'elasticsearch-7',
        |  'hosts' = 'http://localhost:9200',
        |  'index' = 'user',
        |  'sink.bulk-flush.max-actions' = '1'
        |)
        |""".stripMargin)

    val datagenTable: Table = tableEnv.sqlQuery("SELECT id, name, sex, cast(ts as string) as ts FROM datagen")
    tableEnv.toChangelogStream(datagenTable).print()
    //    tableEnv.sqlQuery("SELECT id, name, sex, cast(ts as string) as ts FROM datagen").execute().print()

    datagenTable.executeInsert("esSinkTable")
    //    tableEnv.executeSql("insert into esSinkTable SELECT id, name, sex, cast(ts as string) as ts FROM datagen")

    env.execute("FlinkSqlSupportElasticSearch")
    Thread.sleep(100000)
  }

}
