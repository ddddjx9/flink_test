package cn.edu.ustb.window.function;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 这里需要注意的是。输入数据类型是增量聚合的结果传入到了这个方法里面
 * <p>
 *     所以是增量聚合的OUT类型：String
 * </p>
 */
public class MyProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {

    /**
     * 注意这里的迭代器里面只有一条数据，就是增量聚合的结果
     * @param s The key for which this window is evaluated.
     * @param context The context in which the window is being evaluated.
     * @param elements The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     */
    @Override
    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) {
        long startTs = context.window().getStart();
        long endTs = context.window().getEnd();
        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

        long count = elements.spliterator().estimateSize();
        out.collect("key = "+s+"的窗口["+windowStart+", "+windowEnd+"]包含了"+count+"条数据："+elements);
    }
}
