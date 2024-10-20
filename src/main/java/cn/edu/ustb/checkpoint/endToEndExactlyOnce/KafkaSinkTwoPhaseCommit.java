package cn.edu.ustb.checkpoint.endToEndExactlyOnce;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;

public class KafkaSinkTwoPhaseCommit {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 启用检查点。默认为barrier对齐，周期为5s，精准一次，精准一次写入Kafka的要求
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://Hadoop130:8020/chk");
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //TODO 读取Kafka数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("Hadoop130:9092,Hadoop131:9092,Hadoop132:9092")  //指定Kafka节点
                .setGroupId("root")
                .setTopics("topic_1")
                .setValueOnlyDeserializer(new SimpleStringSchema())  //仅仅对value做反序列化，key不要
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();  //设置偏移量

        DataStreamSource<String> kafkaSource = env.fromSource(
                source, WatermarkStrategy.forBoundedOutOfOrderness(
                        Duration.ofSeconds(3)), "KafkaSource");

        //TODO 写出到Kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                //指定kafka端口
                .setBootstrapServers("Hadoop130:9092,Hadoop131:9092,Hadoop132:9092")
                //flink作为发送者，指定序列化器
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                //精准一次，开启2pc
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //精准一次，必须设置事务id前缀
                .setTransactionalIdPrefix("root")
                //精准一次，必须设置事务超时时间，大于checkpoint，并且小于max 15分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 10 * 1000 + "")
                .build();

        kafkaSource.sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
