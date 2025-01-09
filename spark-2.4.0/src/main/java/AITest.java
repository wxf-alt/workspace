import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @Auther: wxf
 * @Date: 2024/12/31 09:56:48
 * @Description: AITest
 * @Version 1.0.0
 */
public class AITest {

    public static void main(String[] args) {

        // 创建SparkSQL 的 SparkSession
        // 创建一个SparkSession对象
        // 创建一个SparkSession对象
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // 创建一个HashSet集合，用于存储Row对象
        HashSet<Row> rows = new HashSet<>();
        // 向集合中添加四个Row对象
        rows.add(RowFactory.create("1", "2", "3"));
        rows.add(RowFactory.create("4", "5", "6"));
        rows.add(RowFactory.create("7", "8", "9"));
        rows.add(RowFactory.create("10", "11", "12"));

        // 将 HashSet 转换为 List
        List<Row> rowsList = new ArrayList<>(rows);

        // 根据集合创建Schema
        StructType schema = new StructType(new StructField[]{
                new StructField("a", DataTypes.StringType, false, Metadata.empty()),
                new StructField("b", DataTypes.StringType, false, Metadata.empty()),
                new StructField("c", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(rowsList, schema);

        // 打印df
        df.show();
        df.printSchema();
        System.out.println(df.count());




    }

}