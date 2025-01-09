package functions03;

import bean.Sensor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: wxf
 * @Date: 2024/6/11 14:58:38
 * @Description: Demo3_Accumulator_Counter  累加器 计数器
 * @Version 1.0.0
 */
public class Demo3_Accumulator_Counter {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> socketDataStream = env.socketTextStream("localhost", 6666);

        // 实现接口
        SingleOutputStreamOperator<Sensor> mapDataStream = socketDataStream.map(new MyRichMapFunction());

        mapDataStream.print();

        JobExecutionResult myJobExecutionResult = env.execute("Demo2_RichMapFunction");
        Integer accumulatorResult = myJobExecutionResult.<Integer>getAccumulatorResult("num-lines");

        // 看不到：当前累加器的结果只有在整个作业结束后才可用
        System.out.println("accumulatorResult --> " + accumulatorResult);

    }


    static class MyRichMapFunction extends RichMapFunction<String, Sensor> {

        private IntCounter numLines = new IntCounter();

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建累加器
            getRuntimeContext().addAccumulator("num-lines", numLines);
        }

        @Override
        public Sensor map(String value) throws Exception {
            this.numLines.add(1);
            String[] str = value.split(" ");
            return new Sensor(Integer.parseInt(str[0]), str[1], Double.parseDouble(str[2]));
        }

    }

}