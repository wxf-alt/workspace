import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
    val sumUDAF: SumUDAF = new SumUDAF

    val result: DataFrame = frame
      .groupBy($"id")
      .agg(
        concatenateUDAF($"name").as("concat_name"),
        concatenateUDAF($"age").as("concat_age"),
        sumUDAF($"age").as("sum_age")
      )
      .withColumn("age_sum", ageSumFunction($"concat_age"))

    result.show()

  }

  val ageSumFunction: UserDefinedFunction = udf((concat_age: String) => {
    concat_age.split("_").map(_.toInt).sum
  })

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

  class SumUDAF extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = StructType(Array(StructField("value", IntegerType)))

    override def bufferSchema: StructType = StructType(Array(StructField("buffer", IntegerType)))

    override def dataType: DataType = IntegerType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = 0

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        if (0 == buffer.getInt(0)) {
          buffer(0) = input.getInt(0)
        } else {
          buffer(0) = buffer.getInt(0) + input.getInt(0)
        }
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      if (!buffer2.isNullAt(0)) {
        if (0 == buffer1.getInt(0)) {
          buffer1(0) = buffer2.getInt(0)
        } else {
          buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
        }
      }
    }

    override def evaluate(buffer: Row): Int = buffer.getInt(0)
  }


}
