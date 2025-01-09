package state04;

import bean.Sensor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * @Date: 2024/6/11 15:21:41
 * @Description: Demo1_KeyedState  使用 Keyed State
 *                  这个例子实现了一个简单的计数窗口。 我们把元组的第一个元素当作 key（在示例中都 key 都是 “1”）。
 *                  该函数将出现的次数以及总和存储在 “ValueState” 中。 一旦出现次数达到 2，则将平均值发送到下游，并清除状态重新开始。
 *                  请注意，我们会为每个不同的 key（元组中第一个元素）保存一个单独的值。
 * @Version 1.0.0
 */
public class Demo1_KeyedState {
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

        env.execute("Demo1_KeyedState");
    }

    static class CountWindowAverage extends RichFlatMapFunction<Sensor, Tuple2<Long, Double>> {

        transient ValueState<Tuple2<Long, Double>> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Double>> descriptor = new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {
            }), new Tuple2<Long, Double>(0L, 0.0D));
            // 设置 状态 可查询
            descriptor.setQueryable("query_stat");
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