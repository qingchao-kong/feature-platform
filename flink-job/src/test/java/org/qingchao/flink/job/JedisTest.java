package org.qingchao.flink.job;

import org.junit.Test;
import org.qingchao.flink.job.repo.ClientFactory;
import redis.clients.jedis.Jedis;

/**
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/25 11:53 上午
 */
public class JedisTest {

    final Jedis jedis = ClientFactory.getFlinkJedisClient().getResource();

    @Test
    public void test01(){
        System.out.println(jedis.zrangeByScore("testSumFunction201460988054200030", "-inf", "+inf"));
    }
}
