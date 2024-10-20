package cn.edu.ustb.checkpoint.endToEndExactlyOnce;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;

public class KafkaSinkEOSTwoPC {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("Hadoop130:9092,Hadoop131:9092,Hadoop132:9092")  //指定Kafka节点
                .setGroupId("root")  //指定消费者组的id
                .setTopics("ws")  //指定消费者消费的topic
                .setValueOnlyDeserializer(new SimpleStringSchema())  //仅仅对value做反序列化，key不要
                .setStartingOffsets(OffsetsInitializer.earliest())  //flink消费kafka的策略
                //TODO 作为下游消费者，要设置事务的隔离级别为 读已提交
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        env.fromSource(source, WatermarkStrategy.forBoundedOutOfOrderness(
                        Duration.ofSeconds(3)),
                "kafkaSource")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
