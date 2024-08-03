package cn.edu.ustb.sourceOperator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSource02_Files {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 新的source架构
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path("input\\word.txt"))
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "fileSource")
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
