package org.qingchao.flink.job.streamfunction.slidingwindow;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Map;

import static org.qingchao.flink.job.constant.Constant._CONFIG_TYPE;
import static org.qingchao.flink.job.streamfunction.ResourceInitRichMapFunction.*;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-19 11:07 上午
 */
public class SlidingWindowFilterFunction implements FilterFunction<Map<String, Object>> {
    @Override
    public boolean filter(Map<String, Object> value) throws Exception {
        String _configType = (String) value.getOrDefault(_CONFIG_TYPE, "");
        return get1sFunctionsMap().containsKey(_configType);
    }
}
