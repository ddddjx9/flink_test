package cn.edu.ustb.transformOperator.unionStream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectStreamKeyBy {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );

        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        /*
          实现相互匹配时，
          1. 两条流，不一定哪一条流先来，但是先来的肯定流走了，所以需要存储
          2. 每条流有数据来的时候，存到一个变量中
          3. 每条流有数据来，去另一条流存的变量中查找是否能够匹配
          4. 多并行度下，需要根据关联条件进行keyBy，才能保证key相同的数据进入到同一个子任务中，才能匹配上
         */
        source1.connect(source2)
                .keyBy(s1 -> s1.f0, s2 -> s2.f0)
                .process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {

                    //定义两条流中存储元素的容器
                    final Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
                    final Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

                    /**
                     * 第一条流的处理逻辑
                     * @param value The stream element
                     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
                     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
                     *     timers and querying the time. The context is only valid during the invocation of this
                     *     method, do not store it.
                     * @param out The collector to emit resulting elements to
                     */
                    @Override
                    public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) {
                        Integer id = value.f0;
                        //将数据放入map
                        if (!s1Cache.containsKey(id)) {
                            //如果key不存在，说明是该key的第一条数据，初始化，put进map
                            List<Tuple2<Integer, String>> list = new ArrayList<>();
                            list.add(value);
                            s1Cache.put(id, list);
                        } else {
                            //说明不是第一条数据
                            s1Cache.get(id).add(value);
                        }

                        //从s2中查找是否有能够匹配上的数据
                        //有，就输出；没有，就不输出
                        if (s2Cache.containsKey(id)) {
                            for (Tuple3<Integer, String, Integer> s2 : s2Cache.get(id)) {
                                out.collect("s1: " + value + " <================> " + "s2:" + s2);
                            }
                        }
                    }

                    /**
                     * 第二条流的处理逻辑
                     * @param value The stream element
                     * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
                     *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
                     *     timers and querying the time. The context is only valid during the invocation of this
                     *     method, do not store it.
                     * @param out The collector to emit resulting elements to
                     */
                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) {
                        Integer id = value.f0;
                        //将数据放入map
                        if (!s2Cache.containsKey(id)) {
                            //如果key不存在，说明是该key的第一条数据，初始化，put进map
                            List<Tuple3<Integer, String, Integer>> list = new ArrayList<>();
                            list.add(value);
                            s2Cache.put(id, list);
                        } else {
                            //说明不是第一条数据
                            s2Cache.get(id).add(value);
                        }

                        //从s2中查找是否有能够匹配上的数据
                        //有，就输出；没有，就不输出
                        if (s1Cache.containsKey(id)) {
                            for (Tuple2<Integer, String> s1 : s1Cache.get(id)) {
                                out.collect("s1: " + s1 + " <================> " + "s2:" + value);
                            }
                        }
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
