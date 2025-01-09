package state04;

import bean.Sensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Auther: wxf
 * @Date: 2024/6/12 08:56:35
 * @Description: Demo2_keyedStateTTL  状态有效期 (TTL)  暂时只支持基于 processing time 的 TTL。
 * @Version 1.0.0
 */
public class Demo2_keyedStateTTL {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketDataStream = env.socketTextStream("localhost", 6666);

        SingleOutputStreamOperator<Sensor> mapDataStream = socketDataStream.map(s -> {
            String[] str = s.split(" ");
            return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
        });

        KeyedStream<Sensor, Integer> keyedDataStream = mapDataStream.keyBy(x -> x.getId());

        SingleOutputStreamOperator<Tuple2<Long, Double>> flatMapKeyedStateDataStream = keyedDataStream.flatMap(new CountWindowAverage());

        flatMapKeyedStateDataStream.print();

        env.execute("Demo2_keyedStateTTL");


    }

    static class CountWindowAverage extends RichFlatMapFunction<Sensor, Tuple2<Long, Double>> {

        transient ValueState<Tuple2<Long, Double>> sum;

        @Override
        public void open(Configuration parameters) throws Exception {

            // 设置 状态过期时间
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)   //  仅在创建和写入时更新
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //  不返回过期数据
                    .disableCleanupInBackground() //  关闭后台清理
                    .build();

            ValueStateDescriptor<Tuple2<Long, Double>> descriptor = new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
            }), new Tuple2<Long, Double>(0L, 0.0D));

            // 开启 状态过期时间
            descriptor.enableTimeToLive(ttlConfig);

            sum = getRuntimeContext().<Tuple2<Long, Double>>getState(descriptor);
        }

        @Override
        public void flatMap(Sensor value, Collector<Tuple2<Long, Double>> out) throws Exception {
            Tuple2<Long, Double> stateValue = sum.value();
            if (null != stateValue && 2L == stateValue.f0) {
                out.collect(new Tuple2(value.getId().longValue(), stateValue.f1 / stateValue.f0));
                sum.clear();
            }

            // update the count
            stateValue.f0 += 1;
            // add the second field of the input value
            stateValue.f1 += value.getTemperature();
            // update the state
            sum.update(stateValue);
        }

    }

}