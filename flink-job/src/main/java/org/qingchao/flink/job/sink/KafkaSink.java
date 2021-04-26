package org.qingchao.flink.job.sink;

import com.dianping.cat.Cat;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.qingchao.flink.job.repo.ClientFactory;

import java.util.Objects;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-18 5:18 下午
 */
@Slf4j
public class KafkaSink {
    public static void sink(String topic, String id, String data) {
        final KafkaProducer<String, String> producer = ClientFactory.getBigDataKafkaProducer();
        producer.send(new ProducerRecord<String, String>(topic, id, data), new CallbackImpl(topic, id, data));
        log.info("sink kafka ok, topic:{}", topic);
    }

    @AllArgsConstructor
    static class CallbackImpl implements Callback {
        private String topic;
        private String id;
        private String data;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (Objects.nonNull(metadata) && Objects.isNull(exception)) {
                log.debug("send data to kafka success, topic:{}, id:{}, data:{}, metadata:{}", topic, id, data, metadata);
            } else {
                final String format = String.format("send data to kafka fail, topic:%s, id:%s, data:%s, error:%s", topic, id, data, exception);
                log.error(format, exception);
                Cat.logError(format, exception);
            }
        }
    }
}
