package org.qingchao.flink.job.streamfunction.atom;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Map;

import static org.qingchao.flink.job.constant.Constant._CONFIG_TYPE;
import static org.qingchao.flink.job.streamfunction.ResourceInitRichMapFunction.*;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-01-26 11:40 上午
 */
public class AtomFilterFunction implements FilterFunction<Map<String, Object>> {
    @Override
    public boolean filter(Map<String, Object> value) throws Exception {
        String _configType = (String) value.getOrDefault(_CONFIG_TYPE, "");
        return getAtomFunctionsMap().containsKey(_configType);
    }
}
