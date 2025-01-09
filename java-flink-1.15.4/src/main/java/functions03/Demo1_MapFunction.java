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
 * @Date: 2024/6/11 11:11:52
 * @Description: Demo1_MapFunction
 * @Version 1.0.0
 */
public class Demo1_MapFunction {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketDataStream = env.socketTextStream("localhost", 6666);

        // 实现接口
        SingleOutputStreamOperator<Sensor> mapDataStream = socketDataStream.map(new MyMapFunction());

        // 匿名类
        SingleOutputStreamOperator<Sensor> mapDataStream1 = socketDataStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] str = value.split(" ");
                return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
            }
        });

        // Java 8 Lambdas
        SingleOutputStreamOperator<Sensor> mapDataStream2 = socketDataStream.map(s -> {
            String[] str = s.split(" ");
            return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
        });

        mapDataStream2.print();

        env.execute("Demo1_MapFunction");
    }

    static class MyMapFunction implements MapFunction<String, Sensor> {
        @Override
        public Sensor map(String value) throws Exception {
            String[] str = value.split(" ");
            return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
        }
    }


}