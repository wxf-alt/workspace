package state04;

import bean.Sensor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Auther: wxf
 * @Date: 2024/6/14 16:05:01
 * @Description: Demo5_BroadcastState  广播流使用案例
 * @Version 1.0.0
 */
public class Demo5_BroadcastState {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<Sensor> socketDataStream = env.socketTextStream("localhost", 6666)
                .map(s -> {
                    String[] str = s.split(" ");
                    return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
                });
        ;
        DataStreamSource<String> socketDataStream1 = env.socketTextStream("localhost", 7777);

//        broadCastStream.broadcast(new MapStateDescriptor<String,String>("", new StringSerializer(),new StringSerializer()));
        MapStateDescriptor<String, String> stringStringMapStateDescriptor = new MapStateDescriptor<>("", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        BroadcastStream<String> broadcastStream = socketDataStream1.broadcast(stringStringMapStateDescriptor);

        KeyedStream<Sensor, Integer> keyedStream = socketDataStream.keyBy(x -> x.getId());

        keyedStream.connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<Integer, Sensor, String, Sensor>() {

                    @Override
                    public void processElement(Sensor value, ReadOnlyContext ctx, Collector<Sensor> out) throws Exception {
                        // 进行业务逻辑处理
                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<Sensor> out) throws Exception {
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stringStringMapStateDescriptor);
                        broadcastState.put(value, value);
                    }
                });

    }


}