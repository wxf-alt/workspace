import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession

/**
 * @Auther: wxf
 * @Date: 2024/3/19 09:34:11
 * @Description: SparkIcebergManageDemo
 * @Version 1.0.0
 */
object SparkIcebergManageDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkIcebergManageDemo")
      //      //指定hive catalog, catalog名称为iceberg_hive
      //      .config("spark.sql.catalog.iceberg_hive", "org.apache.iceberg.spark.SparkCatalog")
      //      .config("spark.sql.catalog.iceberg_hive.type", "hive")
      //      .config("spark.sql.catalog.iceberg_hive.uri", "thrift://hadoop1:9083")
      //      .config("iceberg.engine.hive.enabled", "true")
      //      //指定hadoop catalog，catalog名称为iceberg_hadoop
      //      .config("spark.sql.catalog.iceberg_hadoop", "org.apache.iceberg.spark.SparkCatalog")
      //      .config("spark.sql.catalog.iceberg_hadoop.type", "hadoop")
      //      .config("spark.sql.catalog.iceberg_hadoop.warehouse", "file:///E:\\A_data\\4.测试数据\\iceberg\\spark-iceberg")
      .getOrCreate()

    import spark.implicits._

    val conf: Configuration = spark.sparkContext.hadoopConfiguration
    //    val conf: Configuration = new Configuration()

    // Hadoop Catalog
    val hadoopCatalog: HadoopCatalog = new HadoopCatalog(conf, "file:///E:\\A_data\\4.测试数据\\iceberg\\spark-iceberg")
    val table: Table = hadoopCatalog.loadTable(TableIdentifier.of("default", "table1"))

    // Hive Catalog
    //    val hiveCatalog: HiveCatalog = new HiveCatalog()
    //    hiveCatalog.setConf(conf)
    //    val properties: util.HashMap[String, String] = new util.HashMap[String, String]()
    //    properties.put("warehouse", "file:///E:\\A_data\\4.测试数据\\iceberg\\spark-iceberg")
    //    properties.put("uri", "thrift://hadoop1:9083")
    //    hiveCatalog.initialize("hive", properties)
    //    hiveCatalog.loadTable(TableIdentifier.of("default", "table1"))


    //    // 快照过期清理
    //    val tsToExpire: Long = System.currentTimeMillis() - (1000 * 60 * 60 * 24)
    //    table.expireSnapshots().expireOlderThan(tsToExpire).commit()
    //
    //    //    SparkActions.get().expireSnapshots(table).expireOlderThan(tsToExpire).execute()
    //
    //    // 删除无效文件
    //    SparkActions.get().deleteOrphanFiles(table).execute()

    // 合并小文件
    SparkActions.get().rewriteDataFiles(table).option("target-file-size-bytes", (1024 * 1024 * 1024).toString).execute()

  }
}
