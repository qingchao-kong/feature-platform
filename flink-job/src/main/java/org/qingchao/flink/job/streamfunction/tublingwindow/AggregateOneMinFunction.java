package org.qingchao.flink.job.streamfunction.tublingwindow;

import com.dianping.cat.Cat;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.qingchao.flink.job.function.IFunction;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.qingchao.flink.job.constant.Constant.*;
import static org.qingchao.flink.job.streamfunction.ResourceInitRichMapFunction.*;

/**
 * 描述:1min窗口聚合
 *
 * @author kongqingchao
 * @create 2020-12-17 5:34 下午
 */
@Slf4j
public class AggregateOneMinFunction implements AggregateFunction<Map<String, Object>, Map<String, Object>, Map<String, Object>> {
    @Override
    public Map<String, Object> createAccumulator() {
        final Map<String, Object> middleFeature_1min = new HashMap<String, Object>() {{
            put(MIDDLE_FEATURE_1MIN, new HashMap<String, Object>());
        }};
        return middleFeature_1min;
    }

    /**
     * 各个subtask内部的计算
     * <p>
     * 逻辑为：各个源数据value与acc之间的聚合
     * 1.源数据value通过 Map<typeConfig, List<IFunction>> 获取消息对应的List<IFunction>
     * 2.遍历Function对value和acc进行聚合
     * 3.最终结果为acc；acc中包含1min中间结果MIDDLE_FEATURE_1MIN
     *
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public Map<String, Object> add(Map<String, Object> value, Map<String, Object> accumulator) {
        log.info("enter aggregateOneMinFunction.add, value:{}, accumulator:{}", value, accumulator);
        preHandleAcc(value, accumulator);
        String _configType = (String) value.get(_CONFIG_TYPE);
        final List<IFunction> functions = get1mFunctionsMap().getOrDefault(_configType, new LinkedList<>());
        functions.forEach(function -> {
            try {
                function.add_1min(value, accumulator);
            } catch (Throwable throwable) {
                final String format = String.format("AggregateOneMinFunction.add error, value:%s, acc:%s, function:%s", value, accumulator, function);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }
        });
        log.info("exit aggregateOneMinFunction.add, value:{}, accumulator:{}", value, accumulator);
        return accumulator;
    }

    @Override
    public Map<String, Object> getResult(Map<String, Object> accumulator) {
        log.info("AggregateOneMinFunction.getResult, accumulator:{}", accumulator);
        return accumulator;
    }

    /**
     * ⚠️merge方法只有在session window时才会被使用⚠️
     * <p>
     * 多个subtask之间的merge
     * <p>
     * 逻辑为：各个subtask中的acc内的kv特征的聚合
     * 1.遍历a中的acc中的MIDDLE_FEATURE_1MIN（每个key为featureNamePrefix），得到每个key对应的Function
     * 2.Function对a、b两个acc进行处理，得到结果acc b
     * 3.最终结果为acc b；b中包含最终的1min中间结果MIDDLE_FEATURE_1MIN
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b) {
        log.info("enter aggregateOneMinFunction.merge, a:{}, b:{}", a, b);
        Map<String, Object> middleFeature_1min = (Map<String, Object>) a.get(MIDDLE_FEATURE_1MIN);
        middleFeature_1min.keySet().forEach(featureNamePrefix -> {
            final IFunction iFunction = get1mFeatureNamePrefixFunctionMap().get(featureNamePrefix);
            try {
                iFunction.merge_1min(a, b);
            } catch (Throwable throwable) {
                final String format = String.format("AggregateOneMinFunction.merge error, a:%s, b:%s, featureNamePrefix:%s", a, b, featureNamePrefix);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }
        });
        log.info("exit aggregateOneMinFunction.merge, a:{}, b:{}", a, b);
        return b;
    }

    public static void preHandleAcc(Map<String, Object> value, Map<String, Object> accumulator) {

        if (!accumulator.containsKey(_ID)) {
            String _id = (String) value.get(_ID);
            _id = _id.substring(_id.indexOf(":") + 1);
            accumulator.put(_ID, _id);
        }
        if (!accumulator.containsKey(_ID_TYPE)) {
            accumulator.put(_ID_TYPE, value.get(_ID_TYPE));
        }
    }
}
