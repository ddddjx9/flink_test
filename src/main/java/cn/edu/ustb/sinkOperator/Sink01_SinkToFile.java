package cn.edu.ustb.sinkOperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class Sink01_SinkToFile {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //每个目录中，都会有并行度个数的文件 在写入
        env.setParallelism(4);

        DataGeneratorSource<String> source = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return value + "";
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(10), Types.STRING);

        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("hdfs://Hadoop130:8020/flinkSinkToFile"), new SimpleStringEncoder<>())
                //输出文件的配置，如前缀或者后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                        .withPartPrefix("root")
                        .withPartSuffix(".log")
                        .build())
                //按照目录分桶 - 每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.systemDefault()))
                //文件滚动策略 - 假设指定滚动策略为10秒，那么写入达到10秒就不会再次写入了 文件最大1M
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "sinkToFile")
                //输出到文件系统
                .sinkTo(fileSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
