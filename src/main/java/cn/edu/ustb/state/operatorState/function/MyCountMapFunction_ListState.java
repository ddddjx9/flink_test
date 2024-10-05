package cn.edu.ustb.state.operatorState.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * 为OperatorListState类定义Map具体实现，并在其中定义具体状态
 */
//TODO 实现CheckpointedFunction接口
public class MyCountMapFunction_ListState implements MapFunction<String, Long>, CheckpointedFunction {
    //TODO 如果定义在富函数中，需要在open方法中进行初始化
    private Long count = 0L;
    private ListState<Long> state;

    @Override
    public Long map(String value) throws Exception {
        return ++count;
    }

    /**
     * 本地变量的持久化：将 本地变量 拷贝到 算子状态中
     *
     * @param context the context for drawing a snapshot of the operator
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState...");
        //TODO 清空算子状态
        state.clear();
        //TODO 将本地变量添加到算子状态中
        state.add(count);
    }

    /**
     * 初始化本地变量：（该方法通常用在程序恢复时）从状态中，将数据添加到本地变量
     *
     * @param context the context for initializing the operator
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //TODO 本地变量仅仅是一个变量，如果程序出现故障，checkpoint做备份，在恢复和重启时，会调用此方法恢复到算子状态
        //      从算子状态中将值填充到本地变量即可
        //      防止程序挂掉重启时本地变量没有值的情况
        System.out.println("initializeState...");
        //获取算子状态
        state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("operatorListState", Types.LONG));

        //从算子状态中将数据拷贝到本地变量
        if (context.isRestored()) {
            Iterable<Long> elements = state.get();
            for (Long element : elements) {
                count += element;
            }
        }
    }
}
