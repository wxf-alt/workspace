import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
 * @Auther: wxf
 * @Date: 2023/10/26 10:16:23
 * @Description: SparkSessionTest
 * @Version 1.0.0
 */
object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSessionTest").config(conf).master("local[*]").getOrCreate()

    import sparkSession.implicits._

    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")

    val mySqlDF: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/db", "student2", properties).persist()
    val mySqlDF1: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/db", "student1", properties).persist(StorageLevel.OFF_HEAP)

    //    // 笛卡尔积
    //    //    val dataFrame: DataFrame = mySqlDF.join(mySqlDF1,Seq(""),"cross")
    //    val dataFrame: DataFrame = mySqlDF.crossJoin(mySqlDF1)
    //    println(dataFrame.count())
    //
    //    val rdd: RDD[Row] = dataFrame.rdd
    //
    //    Thread.sleep(100000000)

    mySqlDF
      .withColumn("datea", current_date())
      .withColumn("timem", current_timestamp())
      .withColumn("timem1", date_format($"timem","yyyy-MM-dd HH:mm:ss"))
      .show(false)

  }
}
