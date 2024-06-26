import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * @Auther: wxf
 * @Date: 2024/3/19 16:52:10
 * @Description: FlinkReadIcebergDemo
 * @Version 1.0.0
 */
public class FlinkReadIcebergDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableLoader tableLoader = TableLoader.fromHadoopTable("file:///E:\\A_data\\4.测试数据\\iceberg\\flink-iceberg\\default\\demo");

        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
//                .startSnapshotId(3821550127947089987L)
                .build();

        stream.print();

        env.execute("Test Iceberg Read");

    }
}