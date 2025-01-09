import java.sql.Date
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Auther: wxf
 * @Date: 2023/10/26 10:16:23
 * @Description: SparkSessionTest
 * @Version 1.0.0
 */
object SparkSessionPartitionsTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSessionTest").config(conf).master("local[*]").getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    import sparkSession.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")

    val mySqlDF: DataFrame = sparkSession.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/db?characterEncoding=utf-8")
      .option("partitionColumn", "sc_id")
      .option("lowerBound", 0)
      .option("upperBound", 6)
      .option("numPartitions", 6)
      //      .option("dbtable", "student2")
      .option("dbtable", "(SELECT s_id, s_name, s_examnum, sc_id from student2) as  student2")
      .option("user", "root")
      .option("password", "root")
      .load()
      .persist()
    mySqlDF.show()
    println("getNumPartitions   ==> " + mySqlDF.rdd.getNumPartitions)
    println("mySqlDF count ==> " + mySqlDF.count())

    val mySqlDF1: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/db", "student2", properties).persist()
    mySqlDF1.show()
    println("getNumPartitions   ==> " + mySqlDF1.rdd.getNumPartitions)
    println("mySqlDF1 count ==> " + mySqlDF1.count())



    Thread.sleep(100000000)


  }

}
