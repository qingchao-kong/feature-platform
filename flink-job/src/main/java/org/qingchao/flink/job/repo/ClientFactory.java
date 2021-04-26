package org.qingchao.flink.job.repo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.qingchao.flink.job.config.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Objects;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-10 11:16 上午
 */
@Slf4j
public class ClientFactory {

    private volatile static JedisPool flinkJedisPool = null;
    private volatile static JedisPool profileJedisPool = null;

    private volatile static AliHBaseRepo aliHBaseRepo = null;

    private volatile static KafkaProducer<String, String> bigDataKafkaProducer;

    public static AliHBaseRepo getAliHBaseRepo() {
        if (Objects.isNull(aliHBaseRepo)) {
            synchronized (ClientFactory.class) {
                if (Objects.isNull(aliHBaseRepo)) {
                    final HBaseConfig config = ApolloConfigService.getHBaseConfig();
                    org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                    // 集群的连接地址，在控制台页面的数据库连接界面获得(注意公网地址和VPC内网地址)
                    conf.set("hbase.zookeeper.quorum", config.getEndpoint());
                    // 设置用户名密码，默认root:root，可根据实际情况调整
                    conf.set("hbase.client.username", config.getUsername());
                    conf.set("hbase.client.password", config.getPassword());
                    //以毫秒计算的所有HBase RPC超时，默认为60s。

                    //该参数表示一次RPC请求的超时时间。如果某次RPC时间超过该值，客户端就会主动关闭socket。
                    //更改为4秒足够了
                    conf.set("hbase.rpc.timeout", "4000");
                    //该参数表示HBase客户端发起一次数据操作直至得到响应之间总的超时时间(包括异常，客户端对请求发起重试)，
                    // 数据操作类型包括get、append、increment、delete、put等。
                    // 很显然，hbase.rpc.timeout表示一次RPC的超时时间，而hbase.client.operation.timeout则表示一次操作的超时时间，有可能包含多个RPC请求。
                    conf.set("hbase.client.operation.timeout", "6000");
                    //socket建立链接的超时时间
                    conf.set("ipc.socket.timeout", "1000");
                    // 重试此时
                    conf.set("hbase.client.retries.number", "1");
                    //重试间隔时间
                    conf.set("hbase.client.pause", "1000");
                    //zookeeper的重试次数，可调整为3次;
                    conf.set("zookeeper.recovery.retry", "3");
                    conf.set("zookeeper.recovery.retry.intervalmill", "200");
                    aliHBaseRepo = new AliHBaseRepo(conf, config.getPoolSize(), config.getNamespace(), config.getGetRate(), config.getPutRate());
                }
            }
        }
        return aliHBaseRepo;
    }

    /**
     * client集成：https://help.aliyun.com/document_detail/43848.html?spm=a2c4g.11186623.6.628.25db7e90yB5eiJ
     * jedisPool优化：https://help.aliyun.com/document_detail/98726.html?spm=a2c4g.11186623.6.855.445b408cPrXP9N
     *
     * @return
     */
    public static JedisPool getFlinkJedisClient() {
        if (Objects.isNull(flinkJedisPool) || flinkJedisPool.isClosed()) {
            synchronized (ClientFactory.class) {
                if (Objects.isNull(flinkJedisPool) || flinkJedisPool.isClosed()) {
                    final FlinkRedisConfig redisConfig = ApolloConfigService.getFlinkRedisConfig();
                    JedisPoolConfig config = new JedisPoolConfig();
                    {   //todo 参数需要后续review
                        config.setMaxIdle(redisConfig.getMaxIdle());
                        config.setMaxTotal(redisConfig.getMaxActive());
                        config.setMaxWaitMillis(getTime(redisConfig.getMaxWait()));
                        config.setMinIdle(config.getMinIdle());
                    }
                    //使用构造器：JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port, int timeout, final String password, final int database, final String clientName)
                    flinkJedisPool = new JedisPool(config, redisConfig.getHost(), redisConfig.getPort(), redisConfig.getTimeout(),
                            redisConfig.getPassword(), redisConfig.getDatabase(), redisConfig.getClientName());
                }
            }
        }
        return flinkJedisPool;
    }

    public static JedisPool getProfileJedisClient() {
        if (profileJedisPool == null) {
            synchronized (ClientFactory.class) {
                if (profileJedisPool == null) {
                    final ProfileRedisConfig redisConfig = ApolloConfigService.getProfileRedisConfig();
                    JedisPoolConfig config = new JedisPoolConfig();
                    {   //todo 参数需要后续review
                        config.setMaxIdle(redisConfig.getMaxIdle());
                        config.setMaxTotal(redisConfig.getMaxActive());
                        config.setMaxWaitMillis(getTime(redisConfig.getMaxWait()));
                        config.setMinIdle(config.getMinIdle());
                    }
                    //使用构造器：JedisPool(final GenericObjectPoolConfig poolConfig, final String host, int port, int timeout, final String password, final int database, final String clientName)
                    profileJedisPool = new JedisPool(config, redisConfig.getHost(), redisConfig.getPort(), redisConfig.getTimeout(),
                            redisConfig.getPassword(), redisConfig.getDatabase(), redisConfig.getClientName());
                }
            }
        }
        return profileJedisPool;
    }

    public static KafkaProducer<String, String> getBigDataKafkaProducer() {
        if (Objects.isNull(bigDataKafkaProducer)) {
            synchronized (ClientFactory.class) {
                if (Objects.isNull(bigDataKafkaProducer)) {
                    final KafkaConfig kafkaConfig = ApolloConfigService.getkafkaConfig();
                    bigDataKafkaProducer = new KafkaProducer<String, String>(kafkaConfig.getProps());
                }
            }
        }
        return bigDataKafkaProducer;
    }

    public static Long getTime(String mills) {
        int i;
        for (i = mills.length() - 1; i >= 0; i--) {
            if (mills.charAt(i) >= '0' && mills.charAt(i) <= '9') {
                break;
            }
        }
        return Long.parseLong(mills.substring(0, i + 1));
    }
}
