package connector

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Auther: wxf
 * @Date: 2023/9/18 10:58:58
 * @Description: SparkSqlSupportElasticSearch
 * @Version 1.0.0
 */
object SparkSqlSupportElasticSearch {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
    val session: SparkSession = SparkSession.builder().config(sparkConf).master("local[*]").getOrCreate()

    val personsList: util.ArrayList[Person] = new util.ArrayList[Person]()
    personsList.add(Person("林一", "林一", 18))
    personsList.add(Person("李白", "李白", 20))
    personsList.add(Person("昕昕", "昕昕", 17))
    import scala.collection.convert.wrapAll._
    val rdd: RDD[Person] = session.sparkContext.parallelize(personsList)
    val dataFrame: DataFrame = session.createDataFrame(rdd)

    dataFrame.show()

    import org.elasticsearch.spark.sql._
    dataFrame.saveToEs("person/user")


  }

  case class Person(name: String, surname: String, age: Int)

}
