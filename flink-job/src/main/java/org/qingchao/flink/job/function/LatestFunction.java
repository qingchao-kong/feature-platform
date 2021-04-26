package org.qingchao.flink.job.function;

import com.dianping.cat.Cat;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.qingchao.flink.job.config.FlinkConfigDto;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.qingchao.flink.job.constant.Constant.KV;
import static org.qingchao.flink.job.constant.Constant.MIDDLE_FEATURE_1MIN;

/**
 * 描述:最新值
 *
 * @author kongqingchao
 * @create 2021-01-08 10:49 上午
 */
@Slf4j
public class LatestFunction extends AbstractFunction {

    private String field;
    private String eventTimeField;

    public LatestFunction(FlinkConfigDto config) {
        super(config);
        this.field = config.getAggConfig().getString(FIELD);
        this.eventTimeField = config.getAggConfig().getString(EVENT_TIME_FIELD);
    }

    @Override
    public void add_1min(Map<String, Object> value, Map<String, Object> accumulator) {
        final Object fieldExecute = AviatorEvaluator.execute(field, value, true);
        final Object eventTimeExecute = AviatorEvaluator.execute(eventTimeField, value, true);
        //value中值为null时或时间戳为null时过滤，其他情况不过滤
        if (Objects.isNull(fieldExecute) || Objects.isNull(eventTimeExecute)) {
            return;
        }
        final String fieldValue = fieldExecute.toString();
        final long eventTimeValue = Long.parseLong(eventTimeExecute.toString());

        Map<String, Object> middleFeature_1min = (Map) accumulator.get(MIDDLE_FEATURE_1MIN);

        //acc中已有值
        if (middleFeature_1min.containsKey(featureNamePrefix)) {
            Tuple2<String, Long> tuple2 = (Tuple2<String, Long>) middleFeature_1min.get(featureNamePrefix);
            if (eventTimeValue > tuple2.f1) {
                tuple2.setField(eventTimeValue, 1);
            }
        } else {
            //acc中无值，直接put
            middleFeature_1min.put(featureNamePrefix, Tuple2.of(fieldValue, eventTimeValue));
        }
    }

    @Override
    public void merge_1min(Map<String, Object> aAcc, Map<String, Object> bAcc) {
        Map<String, Object> aMiddleFeature_1min = (Map) aAcc.get(MIDDLE_FEATURE_1MIN);
        Map<String, Object> bMiddleFeature_1min = (Map) bAcc.get(MIDDLE_FEATURE_1MIN);
        //a不包含中间特征
        if (!aMiddleFeature_1min.containsKey(featureNamePrefix)) {
            return;
        }

        //b不包含中间特征，a中值put到b中
        if (!bMiddleFeature_1min.containsKey(featureNamePrefix)) {
            bMiddleFeature_1min.put(featureNamePrefix, aMiddleFeature_1min.get(featureNamePrefix));
            return;
        }

        //a、b都包含中间特征
        Tuple2<String, Long> tuple2A = (Tuple2<String, Long>) aMiddleFeature_1min.get(featureNamePrefix);
        Tuple2<String, Long> tuple2B = (Tuple2<String, Long>) bMiddleFeature_1min.get(featureNamePrefix);
        //aEventTime>bEventTime，则替换，否则，不做操作
        if (tuple2A.f1 > tuple2B.f1) {
            tuple2B.setField(tuple2A.f1, 1);
        }
    }

    @Override
    public void kv_1min(Map<String, Object> value) {
        try {
            //1.检查是否存在对应的中间结果
            Map<String, Object> middleFeature_1min = (Map<String, Object>) value.get(MIDDLE_FEATURE_1MIN);
            if (!middleFeature_1min.containsKey(featureNamePrefix)) {
                return;
            }

            //2.拿到中间结果
            Tuple2<String, Long> tuple2 = (Tuple2<String, Long>) middleFeature_1min.get(featureNamePrefix);

            //3.保存到kv中
            final Map<String, Object> kv = (Map<String, Object>) value.getOrDefault(KV, new HashMap<String, Object>());
            kv.put(featureNamePrefix, tuple2.f0);
            value.put(KV, kv);
        } catch (Throwable throwable) {
            final String format = String.format("LatestFunction error, error:%s", throwable);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }
}
