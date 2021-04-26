package org.qingchao.flink.job.streamfunction;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 描述:到时间就触发，并且到时间之前如果已经积累了足够数量的数据；或者在限定时间内没有积累足够数量的数据，触发窗口业务
 *
 * @author kongqingchao
 * @create 2020-12-18 3:44 下午
 */
@Slf4j
public class CountTriggerWithTimeout<T> extends Trigger<T, TimeWindow> {
    /**
     * 窗口最大数据量
     */
    private int maxCount;
    /**
     * event time / process time
     */
    private TimeCharacteristic timeType;

    /**
     * 用于储存窗口当前数据量的状态对象
     */
    private ReducingStateDescriptor<Long> countStateDescriptor =
            new ReducingStateDescriptor("counter", new Sum(), LongSerializer.INSTANCE);

    public CountTriggerWithTimeout(int maxCount, TimeCharacteristic timeType) {

        this.maxCount = maxCount;
        this.timeType = timeType;
    }


    private TriggerResult fireAndPurge(TimeWindow window, TriggerContext ctx) throws Exception {
        clear(window, ctx);
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.add(1L);

        if (countState.get() >= maxCount) {
            log.debug("fire with count: " + countState.get());
            return fireAndPurge(window, ctx);
        }
        if (timestamp >= window.getEnd()) {
            log.debug("fire with tiem: " + timestamp);
            return fireAndPurge(window, ctx);
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.ProcessingTime) {
            return TriggerResult.CONTINUE;
        }

        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            log.debug("fire with process tiem: " + time);
            return fireAndPurge(window, ctx);
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.EventTime) {
            return TriggerResult.CONTINUE;
        }

        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            log.debug("fire with event tiem: " + time);
            return fireAndPurge(window, ctx);
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.clear();
    }

    /**
     * 计数方法
     */
    class Sum implements ReduceFunction<Long> {

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

}
