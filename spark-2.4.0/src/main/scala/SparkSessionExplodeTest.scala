import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @Auther: wxf
 * @Date: 2024/3/11 15:49:52
 * @Description: SparkSessionExplodeTest  测试 explode 函数
 * @Version 1.0.0
 */
object SparkSessionExplodeTest {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    val sparkSession: SparkSession = SparkSession.builder().appName("SparkSessionTest").config(conf).master("local[*]").getOrCreate()

    import sparkSession.implicits._

    val rdd1: RDD[(String, String)] = sparkSession.sparkContext.parallelize(Seq("a" -> "1,2,3,4,5"))
    val rdd2: RDD[(String, String)] = sparkSession.sparkContext.parallelize(Seq("b" -> "6,7"))
    val rdd3: RDD[(String, String)] = sparkSession.sparkContext.parallelize(Seq("a" -> "8"))

    val rdd4: RDD[(String, String)] = sparkSession.sparkContext.parallelize(Seq("a" -> "8"))

    rdd1.persist()
    rdd2.persist()
    rdd3.persist()
    rdd4.persist()

    val df: DataFrame = rdd1.union(rdd2).union(rdd3).union(rdd4).toDF("name", "label")
    df.show()

    val df1: DataFrame = df.withColumn("label-1", explode(split($"label", ",")))
    df1.show()

    /*

        +----+---------+
        |name|    label|
        +----+---------+
        |   a|1,2,3,4,5|
        |   b|      6,7|
        |   a|        8|
        +----+---------+

        +----+---------+-------+
        |name|    label|label-1|
        +----+---------+-------+
        |   a|1,2,3,4,5|      1|
        |   a|1,2,3,4,5|      2|
        |   a|1,2,3,4,5|      3|
        |   a|1,2,3,4,5|      4|
        |   a|1,2,3,4,5|      5|
        |   b|      6,7|      6|
        |   b|      6,7|      7|
        |   a|        8|      8|
        +----+---------+-------+

    */

  }
}
