package cn.edu.ustb.kafkaSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class PollDataFromKafka {
    public static void main(String[] args) {
        // 获取flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置Kafka数据源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("topic_1")
                .setClientIdPrefix("kafka-source-")
                .setGroupId("poll_data_from_kafka")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 读取Kafka数据源
        DataStreamSource<String> sourceDs = env.fromSource(kafkaSource, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(300)), "kafka-source");

        // 处理Kafka数据源，进行排序
        sourceDs.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    out.collect(Tuple2.of(value, 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
                .reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .keyBy(value -> "1")
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(5), Duration.ofSeconds(3)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        // 将元素收集到列表中
                        List<Tuple2<String, Integer>> counts = new ArrayList<>();
                        for (Tuple2<String, Integer> element : elements) {
                            counts.add(element);
                        }

                        // 排序并生成排名
                        counts.sort((a, b) -> Integer.compare(b.f1, a.f1)); // 降序排序
                        for (int i = 0; i < counts.size(); i++) {
                            Tuple2<String, Integer> count = counts.get(i);
                            out.collect("Rank " + (i + 1) + ": " + count.f0 + " - " + count.f1);
                        }
                        System.out.println("-------------------------------------------------------");
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
