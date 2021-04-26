package org.qingchao.flink.job.streamfunction.slidingwindow;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-19 11:32 上午
 */
@Slf4j
public class AggregateSlidingWindowFunction implements AggregateFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> {
    @Override
    public Map<String, Object> createAccumulator() {
        return null;
    }

    @Override
    public Map<String, Object> add(Map<String, Object> value, Map<String, Object> accumulator) {
        return value;
    }

    @Override
    public Map<String, Object> getResult(Map<String, Object> accumulator) {
        log.debug("AggregateSlidingWindowFunction.getResult, acc:{}", accumulator);
        return accumulator;
    }

    @Override
    public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
        return a;
    }
}
