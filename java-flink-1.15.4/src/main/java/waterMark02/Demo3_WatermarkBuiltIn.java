package waterMark02;

import bean.Sensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Auther: wxf
 * @Date: 2024/6/11 10:41:14
 * @Description: Demo3_WatermarkBuiltIn  使用 内置 WaterMark
 * @Version 1.0.0
 */
public class Demo3_WatermarkBuiltIn {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketDataStream = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Sensor> mapDataStream = socketDataStream.map(new RichMapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] strArray = value.split(" ");
                return new Sensor(Integer.parseInt(strArray[0]), strArray[1], Double.parseDouble(strArray[2]));
            }
        });

        // 单调递增时间戳分配器
        SingleOutputStreamOperator<Sensor> assignTimestampsAndWatermarks = mapDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        // 数据之间存在最大固定延迟的时间戳分配器
        SingleOutputStreamOperator<Sensor> sensorSingleOutputStreamOperator = mapDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));


    }
}