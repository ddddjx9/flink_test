package cn.edu.ustb.state.operatorState;

import cn.edu.ustb.sourceOperator.WaterSensor;
import cn.edu.ustb.transformOperator.userDefinedFunction.MyMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 水位超过指定的阈值发送告警，阈值可以动态修改
 */
public class OperatorBroadcastState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 7777)
                .map(new MyMapFunction());

        //配置流（用于广播配置）
        DataStreamSource<String> thresholdDS = env.socketTextStream("localhost", 8888);

        //TODO 将 配置流 广播出去，生成带有广播状态的广播流
        MapStateDescriptor<String, Integer> stateDescriptor = new MapStateDescriptor<>("broadcastState", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = thresholdDS.broadcast(stateDescriptor);

        //TODO 将 数据流 和 广播后的配置进行connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);

        //TODO 调用process
        SingleOutputStreamOperator<String> mainStream = sensorBCS.process(new BroadcastProcessFunction<WaterSensor, String, String>() {
            /**
             * 数据流处理方法
             * @param value 数据流中的元素
             * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
             *     querying the current processing/event time and updating the broadcast state. The context
             *     is only valid during the invocation of this method, do not store it.
             * @param out The collector to emit resulting elements to
             */
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //TODO 通过上下文获取状态，取出里面的值（只读，不可修改）
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(stateDescriptor);
                //如果主流的数据先来，那么很有可能第一条是数据流中的数据先来，所以是拿不到广播变量中的值的，判断一下是否为空
                int threshold = broadcastState.get("threshold") == null ? 0 : broadcastState.get("threshold");
                if (threshold < value.getVc()) {
                    ctx.output(new OutputTag<>("WARN",Types.STRING), "水位值超过阈值！WARN！！");
                }

                out.collect(value.toString());
            }

            /**
             * 广播配置流的处理方法
             * @param value 广播配置流中的元素
             * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
             *     current processing/event time and updating the broadcast state. The context is only valid
             *     during the invocation of this method, do not store it.
             * @param out The collector to emit resulting elements to
             */
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
                //TODO 通过上下文获取广播状态，往里面存储数据
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(stateDescriptor);
                broadcastState.put("threshold", Integer.parseInt(value));
            }
        });

        mainStream.print();
        mainStream.getSideOutput(new OutputTag<>("WARN",Types.STRING)).printToErr();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
