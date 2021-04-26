package org.qingchao.flink.job.streamfunction.tublingwindow;

import com.dianping.cat.Cat;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.qingchao.flink.job.function.IFunction;

import java.util.Map;
import java.util.Objects;

import static org.qingchao.flink.job.constant.Constant.MIDDLE_FEATURE_1MIN;
import static org.qingchao.flink.job.constant.Constant.TS;
import static org.qingchao.flink.job.streamfunction.ResourceInitRichMapFunction.*;

/**
 * @author kongqingchao
 * @date 2021/1/4
 */
@Slf4j
public class ProcessMiddleFeature1MinFunction extends ProcessFunction<Map<String, Object>, Map<String, Object>> {
    @Override
    public void processElement(Map<String, Object> value, Context ctx, Collector<Map<String, Object>> out) throws Exception {
        log.info("enter ProcessMiddleFeature1MinFunction.processElement, value:{}", value);
        try {
            Map<String, Object> middleFeature_1min = (Map<String, Object>) value.get(MIDDLE_FEATURE_1MIN);
            middleFeature_1min.keySet().forEach(featureNamePrefix -> {
                try {
                    final IFunction iFunction = get1mFeatureNamePrefixFunctionMap().get(featureNamePrefix);
                    if (Objects.nonNull(iFunction)) {
                        iFunction.kv_1min(value);
                    }
                    value.put(TS, System.currentTimeMillis());
                } catch (Throwable throwable) {
                    final String format = String.format("ProcessMiddleFeature1MinFunction.processElement error, value:%s, featureNamePrefix:%s, FEATURE_NAME_PREFIX_FUNCTION_MAP_1MIN:%s", value, featureNamePrefix, get1mFeatureNamePrefixFunctionMap());
                    log.error(format, throwable);
                    Cat.logError(format, throwable);
                }
            });
            out.collect(value);
            log.info("exit ProcessMiddleFeature1MinFunction.processElement, value:{}", value);
        } catch (Throwable throwable) {
            final String format = String.format("ProcessMiddleFeature1MinFunction.processElement error, value:%s", value);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }
}
