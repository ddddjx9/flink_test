package cn.edu.ustb.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount01_BatchProcessing {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.读取数据
        final DataSource<String> lineDS = env.readTextFile("input\\word.txt");

        //TODO 3.按行切分
        final FlatMapOperator<String, Tuple2<String, Integer>> wordValue = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                final String[] words = s.split(" ");
                //TODO 4.转换数据为二元组
                for (String word : words) {
                    final Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    //TODO 调用采集器
                    collector.collect(wordTuple2);
                }
            }
        });

        //TODO 5.按照word分组
        //      按照第一个元素分组 - 索引0
        final UnsortedGrouping<Tuple2<String, Integer>> wordGroup = wordValue.groupBy(0);

        //TODO 6.分组内聚合
        //      按照第二个元素聚合 - 索引1
        final AggregateOperator<Tuple2<String, Integer>> wordCount = wordGroup.sum(1);
        wordCount.print();
    }
}
