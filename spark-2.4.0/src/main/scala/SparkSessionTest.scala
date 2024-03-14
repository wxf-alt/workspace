import java.sql.Date
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
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

    val mySqlDF: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/db", "student2", properties).orderBy($"s_id").withColumn("ABNOR_TYPE", lit("0102")).limit(10).persist()
    val mySqlDF1: DataFrame = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/db", "student1", properties).persist(StorageLevel.OFF_HEAP)

    //    // 笛卡尔积
    //    //    val dataFrame: DataFrame = mySqlDF.join(mySqlDF1,Seq(""),"cross")
    //    val dataFrame: DataFrame = mySqlDF.crossJoin(mySqlDF1)
    //    println(dataFrame.count())
    //
    //    val rdd: RDD[Row] = dataFrame.rdd
    //
    //    Thread.sleep(100000000)

    //    mySqlDF
    //      .withColumn("datea", current_date())
    //      .withColumn("timem", current_timestamp())
    //      .withColumn("timem1", date_format($"timem","yyyy-MM-dd HH:mm:ss"))
    //      .show(false)

    //    mySqlDF.printSchema()
    //    mySqlDF.show()
    //
    //    val df1: DataFrame = sparkSession.emptyDataset[Student].toDF()
    //
    //    val df2: Dataset[Row] = mySqlDF.union(df1)
    //    df2.printSchema()
    //    df2.show()


    //    mySqlDF.show()
    //
    //    val frame: DataFrame = mySqlDF.select($"s_id", $"s_name", $"s_sex", $"s_examnum",
    //      expr("case when ABNOR_TYPE = '0102' and s_id <= 5 then '0102' when s_id >= 7 then '0103' else ABNOR_TYPE end") as "ABNOR_TYPE",
    //      when($"ABNOR_TYPE" === "0102", "0102-异常").otherwise("0103-异常") as "ABNOR_DESC"
    //    )
    //      .withColumn("ABNOR_DESC", when($"ABNOR_TYPE" === "0102", "0102-异常").otherwise("0103-异常"))
    //
    //    frame.show()

  }

  case class Student(s_id: Int, s_name: String, s_sex: Int, s_birthday: Date, s_examnum: String, sc_id: Int)

}
