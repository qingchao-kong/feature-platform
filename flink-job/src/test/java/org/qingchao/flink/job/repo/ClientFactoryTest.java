package org.qingchao.flink.job.repo;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-02-02 11:20 上午
 */
@Slf4j
public class ClientFactoryTest {

    @Test
    public void JedisPool_Test(){
        final JedisPool flinkJedisClient = ClientFactory.getFlinkJedisClient();
        final Jedis resource = flinkJedisClient.getResource();
        final String set = resource.set("jedisPoolTest", "jedisPoolTest");

        flinkJedisClient.close();
//        final Jedis resource1 = flinkJedisClient.getResource();
//        final String set1 = resource1.set("jedisPoolCloseTest", "jedisPoolCloseTest");

        final JedisPool flinkJedisClient1 = ClientFactory.getFlinkJedisClient();
        final Jedis resource2 = flinkJedisClient1.getResource();
        final String set2 = resource2.set("jedisPoolReOpenTest", "jedisPoolReOpenTest");

        log.info("ok");
    }
}
