import java.sql.Date
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Auther: wxf
 * @Date: 2023/10/26 10:16:23
 * @Description: SparkSessionDemo
 * @Version 1.0.0
 */
object SparkSessionDemo {

  val conf: SparkConf = new SparkConf()
  val sparkSession: SparkSession = SparkSession.builder().appName("SparkSessionDemo")
    .config(conf)
    .config("spark.sql.crossJoin.enabled", "true")
    .master("local[*]").getOrCreate()

  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {

    //    val df1: DataFrame = sparkSession.sql("SELECT 1 AS ID UNION SELECT 3 AS ID")
    //    val df2: DataFrame = sparkSession.sql("SELECT 1 AS ID, '01' AS TYPE, '2024-04-01' AS TIME UNION SELECT 1 AS ID, '02' AS TYPE, '2024-04-02' AS TIME ")
    //    val df3: DataFrame = sparkSession.sql("SELECT 1 AS ID, '03' AS TYPE, '2024-04-01' AS TIME ")
    //
    //    df1.show()
    //    df2.show()
    //    df3.show()
    //
    //    val df4: DataFrame = df1.as("A")
    //      .join(df2.select($"ID", $"TYPE".as("B_TYPE"), $"TIME".as("B_TIME")), Seq("ID"), "left_outer")
    //      .join(df3.select($"ID", $"TYPE".as("C_TYPE"), $"TIME".as("C_TIME")), Seq("ID"), "left_outer")
    //      .select($"A.ID", $"B_TYPE", $"B_TIME", $"C_TYPE", $"C_TIME")
    //    df4.show()
    //
    //    val df5: DataFrame = df4
    //      .withColumn("max_time", when($"B_TIME" > $"C_TIME", $"B_TIME").otherwise($"C_TIME"))
    //      .withColumn("max_type", when($"B_TIME" > $"C_TIME", $"B_TYPE").otherwise($"C_TYPE"))
    //      .drop("B_TYPE", "B_TIME", "C_TYPE", "C_TIME")
    //    df5.show()
    //
    //    df5
    //      .withColumn("rn", row_number() over Window.partitionBy("ID").orderBy($"max_time".desc))
    //      .filter($"rn" === 1)
    //      .drop("rn")
    //      .show()

    val meterPowerOffPointDf: DataFrame = getMeterPowerOffPoint(sparkSession)
    meterPowerOffPointDf.orderBy($"METER_ID").show()

    //    val df1: DataFrame = sparkSession.sql(
    //      "SELECT 8201000092994090 AS METER_ID, 24 AS UA_CNT, 48 AS UB_CNT, 96 AS UC_CNT, 288 AS IA_CNT, 1440 AS IB_CNT, 20 AS UA_THRESHOLD_CNT, 40 AS UB_THRESHOLD_CNT, 90 AS UC_THRESHOLD_CNT, 280 AS IA_THRESHOLD_CNT, 1400 AS IB_THRESHOLD_CNT, 0 AS IC_CNT, 0 AS IC_THRESHOLD_CNT UNION " +
    //        "SELECT 8201000092994092 AS METER_ID, 1440 AS UA_CNT, 96 AS UB_CNT, 288 AS UC_CNT, 24 AS IA_CNT, 0 AS IB_CNT, 1400 AS UA_THRESHOLD_CNT, 90 AS UB_THRESHOLD_CNT, 280 AS UC_THRESHOLD_CNT, 20 AS IA_THRESHOLD_CNT, 0 AS IB_THRESHOLD_CNT, 48 AS IC_CNT, 40 AS IC_THRESHOLD_CNT ")
    //    df1.show()
    //
    //    // 停电影响点数 赋值 ->
    //    df1.join(meterPowerOffPointDf, Seq("METER_ID"), "left_outer")
    //      .withColumn("POWER_UA_CNT", $"UA_CNT" - floor(getPowerPoint($"UA_CNT", $"MINUTES_DIFFERENCE")))
    //      .show()


  }

  val getPowerPoint: UserDefinedFunction = udf((cnt: Long, difference_minutes: Double) => {
    cnt match {
      case 24 => difference_minutes / 60
      case 48 => difference_minutes / 30
      case 96 => difference_minutes / 15
      case 288 => difference_minutes / 5
      case 1440 => difference_minutes / 1
      case _ => 0.0D
    }
  })

  private def getMeterPowerOffPoint(sparkSession: SparkSession): DataFrame = {
    //    val df1: DataFrame = sparkSession.sql(
    //      "SELECT 8201000092994090 AS METER_ID, '2024-04-07 10:45:24' AS POWEROFF_TIME, '' AS POWERON_TIME UNION " +
    //        "SELECT 8201000092994090 AS METER_ID, '2024-04-07 14:28:24' AS POWEROFF_TIME, '2024-04-08 09:57:24' AS POWERON_TIME UNION " +
    //        "SELECT 8201000092994092 AS METER_ID, '2024-04-07 10:57:24' AS POWEROFF_TIME, '2024-04-07 17:58:52' AS POWERON_TIME UNION " +
    //        "SELECT 8201000092994092 AS METER_ID, '2024-04-07 18:05:20' AS POWEROFF_TIME, '' AS POWERON_TIME")
    //      .withColumn("POWEROFF_TIME", $"POWEROFF_TIME".cast(TimestampType))
    //      .withColumn("POWERON_TIME", $"POWERON_TIME".cast(TimestampType))
    //    df1.orderBy($"METER_ID", $"POWEROFF_TIME").show()

    val df1: DataFrame = sparkSession.sql("SELECT 8201000092994090 AS METER_ID, '2024-04-07 10:50:24' AS POWEROFF_TIME, '2024-04-07 11:15:24' AS POWERON_TIME UNION " +
      "SELECT 8201000092994092 AS METER_ID, '2024-04-07 10:50:24' AS POWEROFF_TIME, '2024-04-07 11:14:24' AS POWERON_TIME")
      .withColumn("POWEROFF_TIME", $"POWEROFF_TIME".cast(TimestampType))
      .withColumn("POWERON_TIME", $"POWERON_TIME".cast(TimestampType))
    df1.orderBy($"METER_ID", $"POWEROFF_TIME").show()


    val df2: DataFrame = df1.orderBy($"METER_ID", $"POWEROFF_TIME")
      // 复电时间为空 置为 下一条的 停电时间
      .withColumn("POWERON_TIME", when($"POWERON_TIME".isNotNull, $"POWERON_TIME").otherwise(lead($"POWEROFF_TIME", 1) over Window.partitionBy($"METER_ID").orderBy($"POWEROFF_TIME")))
      // 重新赋值 复电时间
      .withColumn("POWERON_TIME1",
        when(to_date($"POWERON_TIME", "yyyy-MM-dd") === to_date($"POWEROFF_TIME", "yyyy-MM-dd"), $"POWERON_TIME")
          .otherwise(concat(to_date($"POWEROFF_TIME", "yyyy-MM-dd"), lit(" 23:59:59")).cast(TimestampType))) // 停电超一天 将复电时间修改成当天最大时间
      .drop("POWERON_TIME")
      .withColumnRenamed("POWERON_TIME1", "POWERON_TIME")
    //    df2.orderBy($"METER_ID", $"POWEROFF_TIME").show()

    // 计算停复电时间差 - 分钟级
    val df3: Dataset[Row] = df2
      .withColumn("minutes_difference", (unix_timestamp($"POWERON_TIME") - unix_timestamp($"POWEROFF_TIME")) / 60)
      .orderBy($"METER_ID", $"POWEROFF_TIME")
    //    df3.orderBy($"METER_ID", $"POWEROFF_TIME").show()

    // 计算一个电表停电一共多久   除以采集频次 向上取整 计算出 停电影响点数
    df3
      .groupBy($"METER_ID").agg(sum($"minutes_difference") as "minutes_difference")
      .withColumn("minutes_difference", when($"minutes_difference" >= 1440, 1440).otherwise($"minutes_difference"))
      .withColumn("POINT24", ceil($"minutes_difference" / 60))
      .withColumn("POINT48", ceil($"minutes_difference" / 30))
      .withColumn("POINT96", ceil($"minutes_difference" / 15))
      .withColumn("POINT288", ceil($"minutes_difference" / 5))
      .withColumn("POINT1440", ceil($"minutes_difference" / 1))
//      .withColumn("POINT24", floor($"minutes_difference" / 60))
//      .withColumn("POINT48", floor($"minutes_difference" / 30))
//      .withColumn("POINT96", floor($"minutes_difference" / 15))
//      .withColumn("POINT288", floor($"minutes_difference" / 5))
//      .withColumn("POINT1440", floor($"minutes_difference" / 1))
    //      .orderBy($"METER_ID").show()
  }

}
