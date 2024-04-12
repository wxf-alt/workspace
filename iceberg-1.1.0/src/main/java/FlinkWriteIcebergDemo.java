import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

/**
 * @Auther: wxf
 * @Date: 2024/3/19 15:11:09
 * @Description: FlinkWriteIcebergDemo
 * @Version 1.0.0
 */
public class FlinkWriteIcebergDemo {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        env.enableCheckpointing(5000);

        // 断开算子链
        env.disableOperatorChaining();

        SingleOutputStreamOperator<RowData> inputStram = env
                .socketTextStream("localhost", 6666).uid("source-001").name("source")
                .map(getMapper()).uid("map").name("map-002");
        inputStram.print().uid("print").name("print-003");

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        TableLoader tableLoader = TableLoader.fromHadoopTable("file:///E:\\A_data\\4.测试数据\\iceberg\\flink-iceberg\\default\\demo", hadoopConf);

        FlinkSink.forRowData(inputStram)
                .tableLoader(tableLoader)
                .append();

        env.execute("Test Iceberg DataStream");

    }

    public static MapFunction<String, RowData> getMapper() {
        return (MapFunction<String, RowData>) s -> {
            RowData data = GenericRowData.of(0, new BinaryStringData("0"));
            if (null != s) {
                String[] split = s.split(",");
                if (split.length == 2) {
                    try {
                        data = GenericRowData.of(Integer.parseInt(split[0]), new BinaryStringData(split[1]));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
            }
            return data;
        };
    }
}