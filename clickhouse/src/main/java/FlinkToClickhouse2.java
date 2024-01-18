import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @Auther: wxf
 * @Date: 2023/12/13 17:03:49
 * @Description: FlinkToClickhouse
 * @Version 1.0.0
 */
public class FlinkToClickhouse2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        DataStream<String> dataStream = environment.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Table> mapDataStream = dataStream.map(new RichMapFunction<String, Table>() {
            SimpleDateFormat dateFormat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public Table map(String s) throws Exception {
                String[] str = s.split(",");
                Date date = new Date(dateFormat.parse(str[2]).getTime());
                Table table = new Table(Integer.parseInt(str[0]), str[1], date, Double.parseDouble(str[3]));
                return table;
            }
        });

        mapDataStream.print();
        mapDataStream.addSink(getSinkFunction());

        environment.execute();

    }

    private static RichSinkFunction<Table> getSinkFunction() {
        return new RichSinkFunction<Table>() {

            ClickHouseDataSource clickHouseDataSource;
            ClickHouseConnection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                String url = "jdbc:ch://s5.hadoop/default";
                Properties properties = new Properties();
                clickHouseDataSource = new ClickHouseDataSource(url, properties);
                connection = clickHouseDataSource.getConnection();
            }

            @Override
            public void invoke(Table table, Context context) throws Exception {
                try (PreparedStatement ps = connection.prepareStatement(
                        "insert into my_first_table select col1, col2, col3, col4 from input('col1 Int32, col2 String, col3 DateTime, col4 Float32')")) {
                    ps.setInt(1, table.getUser_id()); // col1
                    ps.setString(2, table.getMessage()); // col2, setTimestamp is slow and not recommended
                    ps.setDate(3, table.getTimestamp()); // col3
                    ps.setDouble(4, table.getMetric()); // col4
                    ps.addBatch(); // parameters will be write into buffered stream immediately in binary format
                    int[] batch = ps.executeBatch();// stream everything on-hand into ClickHouse
                }
            }
        };
    }

}