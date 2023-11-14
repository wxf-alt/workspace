import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

    val properties: Properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")

    val mySqlDF: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/db", "student2", properties)
    val mySqlDF1: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/db", "student1", properties)

    // 笛卡尔积
    //    val dataFrame: DataFrame = mySqlDF.join(mySqlDF1,Seq(""),"cross")
    val dataFrame: DataFrame = mySqlDF.crossJoin(mySqlDF1)
    println(dataFrame.count())

    val rdd: RDD[Row] = dataFrame.rdd

    Thread.sleep(100000000)

  }
}
