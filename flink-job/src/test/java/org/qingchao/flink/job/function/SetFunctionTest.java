package org.qingchao.flink.job.function;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.qingchao.flink.job.repo.ClientFactory;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-02-19 11:24 上午
 */
@Slf4j
public class SetFunctionTest {

    final Jedis jedis = ClientFactory.getFlinkJedisClient().getResource();

    @Test
    public void redisZrange_Test(){
        jedis.zadd("test",1,"1");
        jedis.zadd("test",2,"2");
        jedis.zadd("test",3,"3");
        jedis.zadd("test",4,"4");
        jedis.zadd("test",5,"5");
        jedis.zadd("test",6,"6");
        jedis.zadd("test",7,"7");
        jedis.zadd("test",8,"8");

        final Set<String> zrange1 = jedis.zrevrangeByScore("test", 5, 2);
        final Set<String> zrange2 = jedis.zrangeByScore("test", 2, 5);

        log.info(zrange1.toString());
        log.info(zrange2.toString());
    }
}
