package connector

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
 * @Auther: wxf
 * @Date: 2023/9/18 09:36:20
 * @Description: SparkSupportElasticSearch
 * @Version 1.0.0
 */
object SparkSupportElasticSearch {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSupportElasticSearch")
    //    sparkConf.set("es.nodes", "localhost") // 默认
    //    sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.auto.create", "true")
    val sc: SparkContext = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    //    // 读取 Es 操作
    //    import org.elasticsearch.spark._
    //    val rdd: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("spark/docs")
    //    println(rdd.collectAsMap().toBuffer)

    //    // 写入 Spark
    //    val numbers: Map[String, Int] = Map("one" -> 1, "two" -> 2, "three" -> 3)
    //    val airports: Map[String, String] = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    //    val seq: Seq[Map[String, Any]] = Seq(numbers, airports)
    //    val rdd: RDD[Map[String, Any]] = sc.makeRDD(seq)
    //    println(rdd.collect().toBuffer)
    //
    //    import org.elasticsearch.spark._
    //    rdd.saveToEs("spark/docs")

    //    // 带有 id 主键 写入
    //    val upcomingTrip: Trip = Trip("1", "OTP-SFO", "SFO")
    //    val lastWeekTrip: Trip = Trip("2", "MUC-OTP", "OTP")
    //    val rdd: RDD[Trip] = sc.makeRDD(List(upcomingTrip, lastWeekTrip))
    //    println(rdd.collect().toBuffer)
    //    import org.elasticsearch.spark.rdd.EsSpark
    //    EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id")) // 主键相同会自动更新

  }

  case class Trip(id: String, departure: String, arrival: String)

}
