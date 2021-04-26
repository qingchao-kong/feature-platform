package org.qingchao.flink.job.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.googlecode.aviator.AviatorEvaluator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.qingchao.flink.job.config.FlinkConfigDto;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.qingchao.flink.job.constant.Constant.KV;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-01-26 11:01 上午
 */
public class AtomFunction implements IFunction {
    private String featureNamePrefix;
    private List<Tuple2<String, String>> fields;

    public AtomFunction(FlinkConfigDto config) {
        final JSONArray fields = config.getAggConfig().getJSONArray("fields");
        this.featureNamePrefix = config.getFeatureNamePrefix();
        this.fields = fields.stream()
                .map(item -> (JSONObject) item)
                .map(json -> {
                    final String sourceField = json.getString("sourceField");
                    final String featureName = json.getString("featureName");
                    return Tuple2.of(sourceField, featureName);
                }).collect(Collectors.toList());
    }

    public void handle(Map<String, Object> value, Map<String, Object> out) {
        final Map<String, Object> kv = (Map<String, Object>) out.get(KV);
        fields.forEach(tuple -> {
            final Object execute = AviatorEvaluator.execute(tuple.f0, value, true);
            if (Objects.nonNull(execute) && StringUtils.isNotBlank(execute.toString())) {
                kv.put(featureNamePrefix + "_" + tuple.f1, execute);
            }
        });
    }

    @Override
    public String getType() {
        return "atom";
    }

    @Override
    public void add_1min(Map<String, Object> value, Map<String, Object> accumulator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge_1min(Map<String, Object> aAcc, Map<String, Object> bAcc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void kv_1min(Map<String, Object> value) {
        throw new UnsupportedOperationException();
    }
}
