package cn.edu.ustb.sourceOperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSource04_DataGen {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataGeneratorSource<String> source = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "Number:" + value;
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(3), Types.STRING);

        env.fromSource(source, WatermarkStrategy.noWatermarks(),"DataGenSource")
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
