package org.qingchao.flink.job.streamfunction;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.Map;

import static org.qingchao.flink.job.constant.Constant._ID;
import static org.qingchao.flink.job.constant.Constant._ID_TYPE;

/**
 * 描述:keyBy，分组
 *
 * @author kongqingchao
 * @create 2020-12-17 5:18 下午
 */
public class KeySelectorFunction implements KeySelector<Map<String, Object>, String> {
    @Override
    public String getKey(Map<String, Object> value) throws Exception {
        final String _id = (String) value.get(_ID);
        final String _idType = (String) value.get(_ID_TYPE);
        return _id + _idType;
    }
}
