package org.qingchao.flink.job.streamfunction.tublingwindow;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Map;

import static org.qingchao.flink.job.constant.Constant._CONFIG_TYPE;
import static org.qingchao.flink.job.streamfunction.ResourceInitRichMapFunction.*;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-18 2:33 下午
 */
public class OneMinTumblingWindowFilterFunction implements FilterFunction<Map<String, Object>> {
    @Override
    public boolean filter(Map<String, Object> value) throws Exception {
        String _configType = (String) value.getOrDefault(_CONFIG_TYPE, "");
        return get1mFunctionsMap().containsKey(_configType) || get1hFunctionsMap().containsKey(_configType);
    }
}
