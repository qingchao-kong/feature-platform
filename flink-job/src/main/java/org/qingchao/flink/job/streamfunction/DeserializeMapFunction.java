package org.qingchao.flink.job.streamfunction;

import com.alibaba.fastjson.JSON;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

/**
 * 描述:数据反序列化
 *
 * @author kongqingchao
 * @create 2020-12-03 2:16 下午
 */
@Slf4j
public class DeserializeMapFunction implements MapFunction<Tuple3<String, String, String>, Map<String, Object>> {
    @Override
    public Map<String, Object> map(Tuple3<String, String, String> value) {
        log.debug("input source:{}", value);
        Map<String, Object> input = null;
        Transaction transaction = Cat.newTransaction("KafkaRecv", value.f0);
        try {
            input = JSON.parseObject(value.f2, Map.class);
            log.debug("deserialized data:{}", input);
            transaction.setSuccessStatus();
        } catch (Throwable throwable) {
            transaction.setStatus(throwable);
            final String format = String.format("deserialize input error, error:%s", throwable);
            Cat.logError(format, throwable);
            log.error(format, throwable);
        }
        transaction.complete();
        return input;
    }
}
