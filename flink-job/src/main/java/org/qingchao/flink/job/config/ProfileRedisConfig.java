package org.qingchao.flink.job.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfig;
import lombok.Data;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-01-21 11:30 上午
 */
@Data
public class ProfileRedisConfig {
    @ApolloConfig("middleware.redis.aitm-profile-service")
    private Config redisConfig;

    private String host;

    private String password;

    private Integer database;

    private Integer port;

    private Integer maxActive;

    private Integer maxIdle;

    private String maxWait;

    private Integer minIdle;

    private Integer defaultExpire;

    private Boolean localCacheEnable;

    private Integer expireAfterWrite;

    private Boolean enableTransactionSupport;

    private String clientName;

    private Integer timeout;

    public void init() {
        host = redisConfig.getProperty("spring.redis.host", "test");
        password = redisConfig.getProperty("spring.redis.password", "test");
        database = redisConfig.getIntProperty("spring.redis.database", 10);
        port = redisConfig.getIntProperty("spring.redis.port", 6379);
        maxActive = redisConfig.getIntProperty("spring.redis.jedis.pool.max-active", 20);
        maxIdle = redisConfig.getIntProperty("spring.redis.jedis.pool.max-idle", 5);
        maxWait = redisConfig.getProperty("spring.redis.jedis.pool.max-wait", "1000ms");
        minIdle = redisConfig.getIntProperty("spring.redis.jedis.pool.min-idle", 1);
        defaultExpire = redisConfig.getIntProperty("spring.redis.default-expire", 86400);
        localCacheEnable = redisConfig.getBooleanProperty("spring.redis.local-cache.enable", false);
        expireAfterWrite = redisConfig.getIntProperty("spring.redis.local-cache.expire-after-write", 100);
        enableTransactionSupport = redisConfig.getBooleanProperty("spring.redis.enableTransactionSupport", false);
        clientName = redisConfig.getProperty("clientName", "flinkRedis");
        timeout=redisConfig.getIntProperty("spring.redis.timeout",300);
    }
}
