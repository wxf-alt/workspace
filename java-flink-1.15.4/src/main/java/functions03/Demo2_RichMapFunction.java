package functions03;

import bean.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: wxf
 * @Date: 2024/6/11 11:20:57
 * @Description: Demo2_RichMapFunction 富函数 RichFunction
 * @Version 1.0.0
 */
public class Demo2_RichMapFunction {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketDataStream = env.socketTextStream("localhost", 6666);

        // 实现接口
        SingleOutputStreamOperator<Sensor> mapDataStream = socketDataStream.map(new MyMapFunction());

        mapDataStream.print();

        env.execute("Demo2_RichMapFunction");
    }

    static class MyMapFunction extends RichMapFunction<String, Sensor> {
        @Override
        public Sensor map(String value) throws Exception {
            String[] str = value.split(" ");
            return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
        }
    }

}