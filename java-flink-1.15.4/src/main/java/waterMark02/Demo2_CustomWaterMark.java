package waterMark02;

import bean.Sensor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import waterMark02.custom_watermark.CustomBoundedOutOfOrdernessGenerator;
import waterMark02.custom_watermark.CustomPunctuatedAssigner;

import java.time.Duration;

/**
 * @Auther: wxf
 * @Date: 2024/6/7 15:01:01
 * @Description: Demo2_CustomWaterMark  自定义 WaterMark  watermark 的生成方式本质上是有两种：周期性生成和标记生成。
 * @Version 1.0.0
 */
@SuppressWarnings("ALL")
public class Demo2_CustomWaterMark {
    public static void main(String[] args) throws Exception {

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

        // 设置 WaterMark（自定义）
        SingleOutputStreamOperator<Sensor> sensorSingleOutputStreamOperator = mapDataStream
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Sensor>() {
                    @Override
                    public WatermarkGenerator<Sensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new CustomBoundedOutOfOrdernessGenerator();
                    }
                }
                        .withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
                            @Override
                            public long extractTimestamp(Sensor element, long recordTimestamp) {
                                return Long.parseLong(element.getTimeStamp());
                            }
                        }));

        SingleOutputStreamOperator<Sensor> process = sensorSingleOutputStreamOperator.process(new ProcessFunction<Sensor, Sensor>() {
            @Override
            public void processElement(Sensor value, Context ctx, Collector<Sensor> out) throws Exception {
                TimerService timerService = ctx.timerService();
                System.out.println("currentWatermark --" + timerService.currentWatermark());
                out.collect(value);
            }
        });

        process.print();

        env.execute("Demo2_CustomWaterMark");

    }


}