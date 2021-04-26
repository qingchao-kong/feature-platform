package org.qingchao.flink.job.streamfunction;

import org.apache.flink.api.common.functions.FilterFunction;

import java.util.HashMap;
import java.util.Map;

import static org.qingchao.flink.job.constant.Constant.KV;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-19 4:44 下午
 */
public class KvFilterFunction implements FilterFunction<Map<String, Object>> {
    @Override
    public boolean filter(Map<String, Object> value) throws Exception {
        Map<String, Object> kv=(Map<String, Object>)value.getOrDefault(KV,new HashMap<>());
        return kv.size() > 0;
    }
}
