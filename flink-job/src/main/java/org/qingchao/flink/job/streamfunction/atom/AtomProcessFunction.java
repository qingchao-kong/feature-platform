package org.qingchao.flink.job.streamfunction.atom;

import com.dianping.cat.Cat;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.qingchao.flink.job.function.AtomFunction;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.qingchao.flink.job.constant.Constant.*;
import static org.qingchao.flink.job.streamfunction.ResourceInitRichMapFunction.*;

/**
 * @author kongqingchao
 * @date 2021/1/4
 */
@Slf4j
public class AtomProcessFunction extends ProcessFunction<Map<String, Object>, Map<String, Object>> {
    @Override
    public void processElement(Map<String, Object> value, Context ctx, Collector<Map<String, Object>> out) {
        log.debug("enter AtomProcessFunction.processElement, value:{}", value);
        try {
            String _configType = (String) value.get(_CONFIG_TYPE);
            final List<AtomFunction> functions = getAtomFunctionsMap().get(_configType);

            if (!CollectionUtils.isEmpty(functions)) {
                final Map<String, Object> outMap = getOut(value);

                functions.forEach(function -> {
                    function.handle(value, outMap);
                });
                out.collect(outMap);
                log.debug("exit AtomProcessFunction.processElement, outMap:{}", outMap);
            }
        } catch (Throwable throwable) {
            final String format = String.format("AtomProcessFunction.processElement error, value:%s", value);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }

    public static Map<String, Object> getOut(Map<String, Object> value) {
        Map<String, Object> out = new HashMap<>();
        out.put(_ID, value.get(_ID));
        out.put(_ID_TYPE, value.get(_ID_TYPE));
        out.put(_CONFIG_TYPE, value.get(_CONFIG_TYPE));
        out.put(BIZ_TYPE, value.get(BIZ_TYPE));
        out.put(KV, new HashMap<String, Object>());
        out.put(TS, System.currentTimeMillis());

        return out;
    }
}
