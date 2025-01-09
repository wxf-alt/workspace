import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

/**
 * @Auther: wxf
 * @Date: 2025/1/6 15:31:36
 * @Description: UDAFTest
 * @Version 1.0.0
 */
object UDAFTest {
  def main(args: Array[String]): Unit = {

    // 创建SparkSQL 的 SparkSession

    val sc = SparkSession.builder()
      .appName("SparkSQL Example")
      .master("local[1]")
      .getOrCreate()
    sc.sparkContext.setLogLevel("ERROR")

    import sc.implicits._

    // 创建一个HashSet集合，用于存储Row对象
    val frame: DataFrame = sc.createDataFrame(Seq(
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35),
        (2, "David", 40),
        (5, "Eve", 45)
      )).toDF("id", "name", "age")
      .persist()

    frame.show()

    val concatenateUDAF: ConcatenateUDAF = new ConcatenateUDAF

    val result: DataFrame = frame
      .groupBy($"id")
      .agg(concatenateUDAF($"name").as("concat_name"))

    result.show()

  }

  // 创建UDAF函数，传入一个字段，将多行的字段值使用‘_’连接
  class ConcatenateUDAF extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = StructType(Array(StructField("value", StringType)))

    override def bufferSchema: StructType = StructType(Array(StructField("buffer", StringType)))

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = ""

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //      if (!input.isNullAt(0)) {
      //        buffer(0) = buffer.getString(0) + input.getString(0) + "_"
      //      }
      //    }
      if (!input.isNullAt(0)) {
        if ("" == buffer.getString(0)) {
          buffer(0) = input.getString(0)
        } else {
          buffer(0) = buffer.getString(0) + "_" + input.getString(0)
        }
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      if (!buffer2.isNullAt(0)) {
        if ("" == buffer1.getString(0)) {
          buffer1(0) = buffer2.getString(0)
        } else {
          buffer1(0) = buffer1.getString(0) + "_" + buffer2.getString(0)
        }
      }
    }

    override def evaluate(buffer: Row): String = buffer.getString(0) // .stripSuffix("_").stripPrefix("_")
  }


}
