package kafka.consumer.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Auther: wxf
 * @Date: 2023/12/4 14:05:27
 * @Description: CustomConsumerInterceptor   拦截器
 * @Version 1.0.0
 */
public class CustomConsumerInterceptor implements ConsumerInterceptor<String, String> {

    // 消费前 操作
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {

        Set<TopicPartition> topicPartitionSet = records.partitions();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> consumerRecoed = new HashMap<>();

        for (TopicPartition topicPartition : topicPartitionSet) {
            List<ConsumerRecord<String, String>> recordList = records.records(topicPartition);
            List<ConsumerRecord<String, String>> consumerRecordList = recordList.stream().filter(x -> x.value().contains("过滤数据")).collect(Collectors.toList());
            consumerRecoed.put(topicPartition, consumerRecordList);
        }
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(consumerRecoed);

        return consumerRecords;
    }

    // 消费后 操作
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}