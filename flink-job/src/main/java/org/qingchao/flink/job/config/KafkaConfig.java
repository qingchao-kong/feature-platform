package org.qingchao.flink.job.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/4 4:41 下午
 */
@Data
@Slf4j
public class KafkaConfig {
    @ApolloConfig("middleware.kafka-bigdata-batch")
    private Config config;

    private Properties props;

    public void init() {
        props = new Properties();
        props.put("bootstrap.servers", config.getProperty("spring.kafka.bootstrap-servers", ""));
        props.put("enable.auto.commit", true);
        props.put("auto.commit.interval.ms", 5000);
        props.put("auto.offset.reset", "latest");
        props.put("acks", "all");
        props.put("retries", config.getIntProperty("spring.kafka.producer.retries", 30));
        props.put("batch.size", config.getIntProperty("spring.kafka.consumer.max-poll-records", 3000));
        props.put("linger.ms", 1);
        props.put("group.id",config.getProperty("group.id","aitm-profile-flink-job-jobbase"));
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        log.info("props:{}",props);
    }
}
