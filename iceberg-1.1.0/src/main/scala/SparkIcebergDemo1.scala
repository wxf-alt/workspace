import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Auther: wxf
 * @Date: 2024/3/18 19:19:16
 * @Description: SparkIcebergDemo1
 * @Version 1.0.0
 */
object SparkIcebergDemo1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkIcebergDemo1")
      //      //指定hive catalog, catalog名称为iceberg_hive
      //      .config("spark.sql.catalog.iceberg_hive", "org.apache.iceberg.spark.SparkCatalog")
      //      .config("spark.sql.catalog.iceberg_hive.type", "hive")
      //      .config("spark.sql.catalog.iceberg_hive.uri", "thrift://hadoop1:9083")
      //      .config("iceberg.engine.hive.enabled", "true")
      //指定hadoop catalog，catalog名称为iceberg_hadoop
      .config("spark.sql.catalog.iceberg_hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_hadoop.type", "hadoop")
      //      .config("spark.sql.catalog.iceberg_hadoop.warehouse", "file:///E:\\A_data\\4.测试数据\\iceberg\\spark-iceberg")
      .config("spark.sql.catalog.iceberg_hadoop.warehouse", "file:///E:\\A_data\\4.测试数据\\iceberg\\flink-iceberg")
      .getOrCreate()

    import spark.implicits._

    //    val structType: StructType = StructType(Array(StructField("id", IntegerType), StructField("data", StringType)))
    //    val rdd: RDD[Row] = spark.sparkContext.makeRDD(Seq(Row(1,"A"), Row(2,"B")))
    //    val df: DataFrame = spark.createDataFrame(rdd, structType)
    //    // 写入表
    //    df.writeTo("iceberg_hadoop.default.demo").create()

    // 写入表
    //    val df: DataFrame = spark.createDataFrame(Seq(Sample(1, "A", "a"), Sample(2, "B", "b"), Sample(3, "C", "c")))
    //    df.writeTo("iceberg_hadoop.default.table1").create()

    // 追加写入
    //    df.writeTo("iceberg_hadoop.default.table1").append()

    //    df.writeTo("iceberg_hadoop.default.table1")
    //      .tableProperty("write.format.default", "orc")
    //      .partitionedBy($"categorg")
    //      .createOrReplace()

    // 分区替换写入
    //    val df: DataFrame = spark.createDataFrame(Seq(Sample(1, "ABC", "a"), Sample(2, "BCD", "b"), Sample(4, "DEF", "d")))
    //    df.writeTo("iceberg_hadoop.default.table1").overwritePartitions()

    //    // 读取 Iceberg 表
    //    spark.read
    //      .format("iceberg")
    //      .load("file:///E:\\A_data\\4.测试数据\\iceberg\\spark-iceberg\\default\\table1")
    //      .show()
    //
    spark.table("iceberg_hadoop.default.demo").show()

    //    // 查询 元数据
    //        spark.read.format("iceberg").load("iceberg_hadoop.default.table1.files").show()
    //    spark.read.format("iceberg").load("iceberg_hadoop.default.table1.snapshots").show()
    //    spark.read.format("iceberg").load("iceberg_hadoop.default.table1.history").show()
    //    spark.read.format("iceberg").load("iceberg_hadoop.default.table1.manifests").show()
    //    spark.read.format("iceberg").load("iceberg_hadoop.default.table1.partitions").show()

  }

  case class Sample(id: Int, data: String, category: String)

}
