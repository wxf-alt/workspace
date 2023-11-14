package kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

//        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
//        return ThreadLocalRandom.current().nextInt(partitionInfoList.size());

        // 获取数据 atguigu  hello
        String msgValues = value.toString();

        int partition;

        if (msgValues.contains("atguigu")) {
            partition = 0;
        } else {
            partition = 1;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
