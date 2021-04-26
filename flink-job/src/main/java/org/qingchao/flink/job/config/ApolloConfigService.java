package org.qingchao.flink.job.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/4 5:12 下午
 */
public class ApolloConfigService {
    private static ApplicationContext context = new ClassPathXmlApplicationContext("apollo/*.xml");

    /**
     * 从apollo相应的namespace获取jedis配置信息
     */
    public static KafkaConfig getkafkaConfig() {
        KafkaConfig conf = (KafkaConfig) context.getBean("kafkaConfig");
        conf.init();
        return conf;
    }

    public static HBaseConfig getHBaseConfig(){
        HBaseConfig hBaseConfig=(HBaseConfig)context.getBean("hBaseConfig");
        hBaseConfig.init();
        return hBaseConfig;
    }

    public static ApplicationConfig getapplicationConfig() {
        ApplicationConfig conf = (ApplicationConfig) context.getBean("applicationConfig");
        conf.init();
        return conf;
    }

    public static FlinkRedisConfig getFlinkRedisConfig(){
        final FlinkRedisConfig redisConfig = (FlinkRedisConfig) context.getBean("flinkRedisConfig");
        redisConfig.init();
        return redisConfig;
    }

    public static ProfileRedisConfig getProfileRedisConfig(){
        final ProfileRedisConfig redisConfig = (ProfileRedisConfig) context.getBean("profileRedisConfig");
        redisConfig.init();
        return redisConfig;
    }
}
