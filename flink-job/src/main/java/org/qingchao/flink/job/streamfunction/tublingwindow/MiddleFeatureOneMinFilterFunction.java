package org.qingchao.flink.job.streamfunction.tublingwindow;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.HashMap;
import java.util.Map;

import static org.qingchao.flink.job.constant.Constant.MIDDLE_FEATURE_1MIN;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-18 3:54 下午
 */
public class MiddleFeatureOneMinFilterFunction implements FilterFunction<Map<String, Object>> {
    @Override
    public boolean filter(Map<String, Object> value) throws Exception {
        Map<String, Object> _middleFeature_1min = (Map<String, Object>) value.getOrDefault(MIDDLE_FEATURE_1MIN, new HashMap<>());
        return _middleFeature_1min.size() > 0;
    }
}
