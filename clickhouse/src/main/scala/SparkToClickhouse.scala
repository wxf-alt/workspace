import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @Auther: wxf
 * @Date: 2023/12/13 16:53:18
 * @Description: SparkToClickhouse
 * @Version 1.0.0
 */
object SparkToClickhouse {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClickHouse")
    val sc: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sc.implicits._

    val properties: Properties = new Properties
    properties.put("driver", "com.github.housepower.jdbc.ClickHouseDriver")

    val url: String = "jdbc:clickhouse://s5.hadoop:9000/default"

    val dataFrame: DataFrame = sc.read.jdbc(url, "my_first_table", properties)
    dataFrame.show(100, false)

    dataFrame.write.mode(SaveMode.Append).jdbc(url, "my_first_table1", properties)

  }

}
