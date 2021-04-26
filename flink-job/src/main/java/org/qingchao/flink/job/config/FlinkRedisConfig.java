package org.qingchao.flink.job.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfig;
import lombok.Data;

/**
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/9 11:05 上午
 */
@Data
public class FlinkRedisConfig {

    @ApolloConfig("middleware.tair.aitm-profile-flink-job")
    private Config redisConfig;

    private String host;

    private String password;

    private Integer database;

    private Integer port;

    private Integer maxActive;

    private Integer maxIdle;

    private String maxWait;

    private Integer minIdle;

    private Boolean useTairCadCommand;

    private String defaultLockTimeout;

    private String defaultTryLockTimeout;

    private Boolean throwExIfUnlockFail;

    private Integer maxRenewalNumber;

    private Boolean releaseKeyWhenDestroy;

    private Boolean defaultEnableRenewal;

    private Integer renewalSecond;

    private Boolean enableTransactionSupport;

    private String clientName;

    private Integer timeout;

    public void init() {
        host = redisConfig.getProperty("middleware.tair.host", "test");
        password = redisConfig.getProperty("middleware.tair.password", "test");
        database = redisConfig.getIntProperty("middleware.tair.database", 0);
        port = redisConfig.getIntProperty("middleware.tair.port", 6379);
        maxActive = redisConfig.getIntProperty("middleware.tair.jedis.pool.max-active", 20);
        maxIdle = redisConfig.getIntProperty("middleware.tair.jedis.pool.max-idle", 4);
        maxWait = redisConfig.getProperty("middleware.tair.jedis.pool.max-wait", "100ms");
        minIdle = redisConfig.getIntProperty("middleware.tair.jedis.pool.min-idle", 1);

        useTairCadCommand = redisConfig.getBooleanProperty("middleware.tair.lock.useTairCadCommand", true);
        defaultLockTimeout = redisConfig.getProperty("middleware.tair.lock.defaultLockTimeout", "30s");
        defaultTryLockTimeout = redisConfig.getProperty("middleware.tair.lock.defaultTryLockTimeout", "10s");
        throwExIfUnlockFail = redisConfig.getBooleanProperty("middleware.tair.lock.throwExIfUnlockFail", true);
        maxRenewalNumber = redisConfig.getIntProperty("middleware.tair.lock.maxRenewalNumber", 60);
        releaseKeyWhenDestroy = redisConfig.getBooleanProperty("middleware.tair.lock.releaseKeyWhenDestroy", true);
        defaultEnableRenewal = redisConfig.getBooleanProperty("middleware.tair.lock.defaultEnableRenewal", false);
        renewalSecond = redisConfig.getIntProperty("middleware.tair.lock.renewalSecond", 30);
        enableTransactionSupport = redisConfig.getBooleanProperty("middleware.tair.enableTransactionSupport", false);

        clientName = "flinkRedis";
        timeout = 300;
    }
}
