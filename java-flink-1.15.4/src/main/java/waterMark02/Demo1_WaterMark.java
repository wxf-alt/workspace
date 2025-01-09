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
 * @Date: 2024/6/7 11:12:49
 * @Description: Demo1_WaterMark   设置 WaterMark
 * @Version 1.0.0
 */
public class Demo1_WaterMark {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        DataStreamSource<String> socketDataStream = env.socketTextStream("localhost", 6666);
        SingleOutputStreamOperator<Sensor> mapDataStream = socketDataStream.map(new RichMapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] strArray = value.split(" ");
                return new Sensor(Integer.parseInt(strArray[0]), strArray[1], Double.parseDouble(strArray[2]));
            }
        });

        // 设置 WaterMark
        SingleOutputStreamOperator<Sensor> waterMarkDataStream = mapDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Sensor>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 设置最大乱序时间
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.getTimeStamp()))   // 指定时间字段
                .withIdleness(Duration.ofSeconds(10))  // 超时标记为空闲状态
                .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(5))  // 水印对齐
        );
//                .withTimestampAssigner(TimestampAssignerSupplier.of(new SerializableTimestampAssigner<Sensor>() {
//                    @Override
//                    public long extractTimestamp(Sensor element, long recordTimestamp) {
//                        return Long.parseLong(element.getTimeStamp());
//                    }
//                })));

        waterMarkDataStream.print();

        env.execute("Demo1_WaterMark");
    }
}