package cn.edu.ustb.sinkOperator;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class Sink02_SinkToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //必须开启checkpoint，否则在精准一次时无法写入Kafka
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                //指定kafka端口
                .setBootstrapServers("Hadoop130:9092,Hadoop131:9092,Hadoop132:9092")
                //flink作为发送者，指定序列化器
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .<String>builder()
                                .setTopic("waterSensor")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                //发送过去后保证数据不丢失或者不重复 - Kafka的一致性：精准一次，至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //如果是精准一次，必须设置事务id前缀
                .setTransactionalIdPrefix("root")
                //如果是精准一次，必须设置事务超时时间，大于checkpoint，并且小于max 10分钟
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 10 * 1000 + "")
                .build();

        env.socketTextStream("Hadoop132", 7777)
                .sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
