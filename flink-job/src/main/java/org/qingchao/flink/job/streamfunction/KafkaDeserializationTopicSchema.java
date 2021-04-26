package org.qingchao.flink.job.streamfunction;

import com.dianping.cat.Cat;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.nio.charset.StandardCharsets;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-02-01 11:37 上午
 */
@Slf4j
public class KafkaDeserializationTopicSchema implements KeyedDeserializationSchema<Tuple3<String, String, String>> {
    @Override
    public Tuple3<String, String, String> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
        try {
            String key = null;
            String value = null;
            if (messageKey != null) {
                key = new String(messageKey, StandardCharsets.UTF_8);
            }
            if (message != null) {
                value = new String(message, StandardCharsets.UTF_8);
            }

            return Tuple3.of(topic, key, value);
        } catch (Throwable throwable) {
            final String format = String.format("KafkaDeserializationTopicSchema.deserialize error, messageKey:%s, message:%s, topic:%s, partition:%s, offset:%s", messageKey, message, topic, partition, offset);
            log.error(format, throwable);
            Cat.logError(format, throwable);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Tuple3<String, String, String> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Tuple3<String, String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
        });
    }
}
