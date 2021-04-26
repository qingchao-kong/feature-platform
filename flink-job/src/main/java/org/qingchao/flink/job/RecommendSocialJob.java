package org.qingchao.flink.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.qingchao.flink.job.config.ApolloConfigService;
import org.qingchao.flink.job.streamfunction.DeserializeMapFunction;
import org.qingchao.flink.job.streamfunction.KafkaDeserializationTopicSchema;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.qingchao.flink.job.JobBase.addIdType;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-02-22 2:01 下午
 */
@Slf4j
public class RecommendSocialJob {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        Properties properties = ApolloConfigService.getkafkaConfig().getProps();

        //stream 根据判断条件生成多个流

        //1.配置数据源
        //1.1.1.
        final FlinkKafkaConsumer<Tuple3<String, String, String>> consumer = new FlinkKafkaConsumer<Tuple3<String, String, String>>("YPP-REALTIME-FEATURE-GAME", new KafkaDeserializationTopicSchema(), properties);
//        gameConsumer.setStartFromGroupOffsets();
        DataStream<Tuple3<String, String, String>> stream = env.addSource(consumer);
        //1.1.2.数据源预处理
        final SingleOutputStreamOperator<Map<String, Object>> deserialStream = stream
                //deserialize
                .map(new DeserializeMapFunction())
                .filter(Objects::nonNull);

        //1.1.3.数据源划分维度
        final SingleOutputStreamOperator<Map<String, Object>> uidStream = addIdType(deserialStream, "uid");
        final SingleOutputStreamOperator<Map<String, Object>> targetUidStream = addIdType(deserialStream, "targetUid");
        DataStream<Map<String, Object>> allStream = uidStream.union(targetUidStream);
        log.info("starting RecommendSocialJob");
        JobBase.main(env, allStream);
    }
}
