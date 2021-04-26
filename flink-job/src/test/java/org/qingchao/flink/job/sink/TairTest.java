package org.qingchao.flink.job.sink;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.qingchao.flink.job.repo.ClientFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-02-26 10:26 上午
 */
@Slf4j
public class TairTest {

    /**
     * tair事务测试
     */
    @Test
    public void tairTransaction_Test() {
        try (final Jedis jedis = ClientFactory.getProfileJedisClient().getResource()) {
            Map<String, String> map = new HashMap<String, String>() {{
                put("key1", "value1");
                put("key2", "value2");
                put("key3", "value3");
            }};
            jedis.hmset("tairTransactionKey", map);
            jedis.expire("tairTransactionKey", 60 * 60);


            Map<String, String> map1 = new HashMap<String, String>() {{
                put("key4", "value4");
            }};
            final Transaction multi = jedis.multi();
            {
                multi.hmset("tairTransactionKey", map1);
                multi.expire("tairTransactionKey", 3 * 60 * 60);
                multi.exec();
            }

            final Map<String, String> map2 = jedis.hgetAll("tairTransactionKey");

            log.info("map:{}", map2);
        } catch (Throwable throwable) {
            log.error(throwable.toString());
        }
    }

    @Test
    public void zadd_Test() {
        try (final Jedis jedis = ClientFactory.getProfileJedisClient().getResource()) {
            jedis.zadd("zaddKey", 1, "1");
            jedis.zadd("zaddKey", 2, "2");
            jedis.zadd("zaddKey", 3, "3");
            jedis.expire("zaddKey", 100);
            final Long ttl = jedis.pttl("zaddKey");
            jedis.zadd("zaddKey", 4, "4");
            final Long ttl1 = jedis.pttl("zaddKey");

            log.info("ttl:{}, ttl1:{}", ttl, ttl1);
        } catch (Throwable throwable) {
            log.error(throwable.toString());
        }
    }
}
