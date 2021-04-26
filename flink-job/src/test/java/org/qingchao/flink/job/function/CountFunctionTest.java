package org.qingchao.flink.job.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.qingchao.flink.job.config.FlinkConfigDto;
import org.qingchao.flink.job.repo.ClientFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static org.qingchao.flink.job.constant.Constant.*;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-24 10:41 上午
 */
@Slf4j
public class CountFunctionTest {
    @Test
    public void kv_1min_Test() {
        String featureNamePrefix = "testCountFunction";
        final Jedis jedis = ClientFactory.getFlinkJedisClient().getResource();

        //1.mock config
        FlinkConfigDto configDto = new FlinkConfigDto();
        {
            configDto.setFeatureNamePrefix(featureNamePrefix);
            JSONArray windows = new JSONArray();
            {
                JSONObject window1 = new JSONObject();
                {
                    window1.put("delay", "1s");
                    window1.put("window", "5m");
                }
                windows.add(window1);

                JSONObject window2 = new JSONObject();
                {
                    window2.put("delay", "1m");
                    window2.put("window", "10m");
                }
                windows.add(window2);

                JSONObject window3 = new JSONObject();
                {
                    window3.put("delay", "1m");
                    window3.put("window", "3h");
                }
                windows.add(window3);

            }
            configDto.setWindows(windows);
        }

        //2.mock CountFunction
        CountFunction countFunction = new CountFunction(configDto);

        //3.mock redis data
        final long startScore = System.currentTimeMillis();
        final long last5min = startScore - 5 * 60 * 1000;
        final long last30min = startScore - 30 * 60 * 1000;
        final long last1h = startScore - 1 * 60 * 60 * 1000;
        final long last2h = startScore - 2 * 60 * 60 * 1000;
        final long last3h = startScore - 3 * 60 * 60 * 1000;
        final long last4h = startScore - 4 * 60 * 60 * 1000;

        String _id = "201460988054200030";
        Long middleFeature = new Long(3);
        final String zSetKey = featureNamePrefix + _id;

        final String member = startScore + "_" + middleFeature;
        final String memberlast5min = last5min + "_" + middleFeature;
        final String memberlast30min = last30min + "_" + middleFeature;
        final String memberlast1h = last1h + "_" + middleFeature;
        final String memberlast2h = last2h + "_" + middleFeature;
        final String memberlast3h = last3h + "_" + middleFeature;
        final String memberlast4h = last4h + "_" + middleFeature;

        final Long zadd = jedis.zadd(zSetKey, startScore, member);
        final Long zaddlast5min = jedis.zadd(zSetKey, last5min, memberlast5min);
        final Long zaddlast30min = jedis.zadd(zSetKey, last30min, memberlast30min);
        final Long zaddlast1h = jedis.zadd(zSetKey, last1h, memberlast1h);
        final Long zaddlast2h = jedis.zadd(zSetKey, last2h, memberlast2h);
        final Long zaddlast3h = jedis.zadd(zSetKey, last3h, memberlast3h);
        final Long zaddlast4h = jedis.zadd(zSetKey, last4h, memberlast4h);

        jedis.pexpire(zSetKey, 4 * 60 * 60 * 1000);

        //4.mock middleFeature
        Map<String, Object> value = new HashMap<>();
        {
            value.put(_ID, _id);
            Map<String, Object> middleFeature_1min = new HashMap<>();
            {
                middleFeature_1min.put(featureNamePrefix,new Long(3));
            }
            value.put(MIDDLE_FEATURE_1MIN, middleFeature_1min);
        }

        countFunction.kv_1min(value);

        //5.check and assert
        log.info(JSON.toJSONString(value));
        Map<String,Object> kv=(Map<String,Object>)value.get(KV);
        Long feature10min=(Long)kv.get(featureNamePrefix+"_10m");
        Long feature3h=(Long)kv.get(featureNamePrefix+"_3h");

        Assert.assertTrue(feature3h>feature10min);
    }
}
