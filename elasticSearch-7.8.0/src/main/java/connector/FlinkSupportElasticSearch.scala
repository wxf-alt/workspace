package connector

import java.nio.charset.{Charset, StandardCharsets}
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType

import scala.beans.BeanProperty

/**
 * @Auther: wxf
 * @Date: 2023/9/18 11:22:27
 * @Description: FlinkSupportElasticSearch flink写入Es
 * @Version 1.0.0
 */
object FlinkSupportElasticSearch {
  def main(args: Array[String]): Unit = {

    val conf: Configuration = new Configuration()
    conf.set(RestOptions.BIND_PORT, "8081")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val dataStream: DataStream[String] = env.socketTextStream("localhost", 6666)

    val splitDataStream: DataStream[User] = dataStream.map(x => {
      val str: Array[String] = x.split(",")
      User(str(0).toInt, str(1), str(2), str(3).toInt)
    })

    val httpHostList: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHostList.add(new HttpHost("localhost", 9200, "http"))
    val elasticSearchSinkBuilder: ElasticsearchSink.Builder[User] = new ElasticsearchSink.Builder[User](httpHostList, new ElasticsearchSinkFunction[User]() {

      override def process(element: User, ctx: RuntimeContext, indexer: RequestIndexer) = {
        val objectMapper = new ObjectMapper
        val jsonString: String = objectMapper.writeValueAsString(element)
        println("jsonString ==》" + jsonString)

        val indexRequest: IndexRequest = new IndexRequest()
          .index("flink-idea")
          .id(element.id.toString)
          .source(jsonString, XContentType.JSON)
        indexer.add(indexRequest)
      }
    })
    elasticSearchSinkBuilder.setBulkFlushMaxActions(1)

    // 输出
    splitDataStream.addSink(elasticSearchSinkBuilder.build)

    env.execute("FlinkSupportElasticSearch")

  }

  case class User(@BeanProperty id: Int, @BeanProperty name: String, @BeanProperty sex: String, @BeanProperty age: Int)

}
