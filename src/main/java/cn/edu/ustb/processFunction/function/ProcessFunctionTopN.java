package cn.edu.ustb.processFunction.function;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class ProcessFunctionTopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {
    // 存储不同窗口的统计结果，KEY为VC，VALUE为每个窗口的排序结果
    private final Map<Long, List<Tuple3<Integer, Integer, Long>>> map;
    // 通过构造器动态传参，想要展示几条结果
    private final int threshold;

    public ProcessFunctionTopN(int threshold) {
        this.threshold = threshold;
        this.map = new HashMap<>();
    }

    @Override
    public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) {

        // TODO 进入这个方法里面的仅为一条数据，因为process函数仅能处理一条数据，如果想要排序，得存起来一块排序
        if (map.containsKey(value.f2)) {
            // 如果不是第一条数据
            List<Tuple3<Integer, Integer, Long>> result = map.get(value.f2);
            result.add(value);
        } else {
            ArrayList<Tuple3<Integer, Integer, Long>> result = new ArrayList<>();
            result.add(value);
            map.put(value.f2, result);
        }

        // 注册事件时间，相当于闹钟，定点启动
        ctx.timerService().registerEventTimeTimer(value.f2 + 1);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        // 定时器触发，同一个窗口范围的计算结果攒齐了，开始排序，取TopN
        Long windowEnd = ctx.getCurrentKey();
        List<Tuple3<Integer, Integer, Long>> result = map.get(windowEnd);
        result.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
            @Override
            public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                return o2.f1 - o1.f1;
            }
        });

        // 防止因数据不够导致的数组越界问题
        for (int i = 0; i < Math.min(threshold, result.size()); i++) {
            out.collect("当前水位线为：" + result.get(i).f0 + "，出现次数为：" + result.get(i).f1 + "，窗口结束时间为：" + result.get(i).f2);
        }

        // 用完的list，及时清理，节省资源
        result.clear();
    }
}
