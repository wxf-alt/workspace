package structured

import java.sql.{Connection, DriverManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}

/**
 * @Auther: wxf
 * @Date: 2024/5/15 15:11:38
 * @Description: StructuredDemo
 * @Version 1.0.0
 */
object StructuredDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("StructuredDemo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dataDf: DataFrame = spark.readStream
      .option("host", "localhost")
      .option("port", 6666L)
      .format("socket")
      .load()

    val result = dataDf.as[String].flatMap(_.split(" ")).groupBy("value").count().sort($"count".desc)

    result.writeStream.format("console")
      .outputMode("complete") //每次将所有的数据写出
      //      .outputMode("append")
      //      .outputMode("update")
      .trigger(Trigger.ProcessingTime(0)) //触发时间间隔,0表示尽可能的快
      //.option("checkpointLocation","./ckp")//设置checkpoint目录,socket不支持数据恢复,所以第二次启动会报错,需要注掉
      .start() //开启
      .awaitTermination() //等待停止

  }

}
