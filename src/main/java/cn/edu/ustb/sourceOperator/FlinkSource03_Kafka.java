package cn.edu.ustb.sourceOperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSource03_Kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("Hadoop130:9092,Hadoop131:9092,Hadoop132:9092")  //指定Kafka节点
                .setGroupId("root")
                .setTopics("topic_1")
                .setValueOnlyDeserializer(new SimpleStringSchema())  //仅仅对value做反序列化，key不要
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();//设置偏移量

        //TODO 从Kafka数据源读取数据
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
