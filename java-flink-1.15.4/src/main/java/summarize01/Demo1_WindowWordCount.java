package summarize01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Auther: wxf
 * @Date: 2024/6/6 09:17:23
 * @Description: summarize01.Demo1_WindowWordCount 构建环境，代码示例
 * @Version 1.0.0
 */
public class Demo1_WindowWordCount {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8081);

        // 获取一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 加载/创建初始数据
        DataStreamSource<String> source = env.readTextFile("E:\\A_data\\3.code\\workspace\\java-flink-1.15.4\\src\\main\\resources\\apache.txt");

        Thread.sleep(5000);

        // 指定数据相关的转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = source.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] str = value.split(" ");
                Tuple2<String, Integer> tuple2 = new Tuple2<>(str[0], Integer.parseInt(str[1]));
                return tuple2;
            }
        });

        // 指定计算结果的存储位置
        mapStream.print();

        // 触发程序执行
        env.execute("summarize01.Demo1_WindowWordCount");
    }

}