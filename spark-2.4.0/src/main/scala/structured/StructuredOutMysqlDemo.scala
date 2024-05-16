package structured

import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, ForeachWriter, SparkSession}

/**
 * @Auther: wxf
 * @Date: 2024/5/15 15:11:38
 * @Description: StructuredDemo
 * @Version 1.0.0
 */
object StructuredOutMysqlDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("StructuredDemo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dataDf: DataFrame = spark.readStream
      .option("host", "localhost")
      .option("port", 6666L)
      .format("socket")
      .load()

    val result = dataDf.as[String].flatMap(_.split(" "))

    val mysqlSink = MySqlForeachWrite("jdbc:mysql://localhost:3306/db", "root", "root")

    result.writeStream
      .foreach(mysqlSink)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0)) //触发时间间隔,0表示尽可能的快
      .start() //开启
      .awaitTermination() //等待停止


  }

  case class MySqlForeachWrite(url: String, user: String, pwd: String) extends ForeachWriter[String] {

    var conn: Connection = _

    override def open(partitionId: Long, epochId: Long): Boolean = {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(url, user, pwd)
      true
    }

    override def process(value: String): Unit = {
      val p = conn.prepareStatement("insert into test1(a) values(?)")
      p.setString(1, value)
      p.execute()
    }

    override def close(errorOrNull: Throwable): Unit = {
      conn.close()
    }
  }

}
