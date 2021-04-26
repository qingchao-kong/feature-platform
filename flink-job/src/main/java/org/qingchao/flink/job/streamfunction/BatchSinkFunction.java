package org.qingchao.flink.job.streamfunction;

import com.dianping.cat.Cat;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.qingchao.flink.job.sink.HBaseSink;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-18 5:35 下午
 */
@Slf4j
public class BatchSinkFunction extends ProcessWindowFunction<Map<String, Object>, Object, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Map<String, Object>> elements, Collector<Object> out) throws Exception {
        try {
            if (Objects.nonNull(elements) && elements.iterator().hasNext()) {
                elements.forEach(element -> {
                    log.info("sink data:{}", element);
                });
            }
            HBaseSink.sink(elements);
//        KafkaSink.sink(elements);
        } catch (Throwable throwable) {
            final ArrayList<Map<String, Object>> lists = Lists.newArrayList(elements);
            final String format = String.format("BatchSinkFunction.process error, elements:%s, error:%s", lists, throwable);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }
}
