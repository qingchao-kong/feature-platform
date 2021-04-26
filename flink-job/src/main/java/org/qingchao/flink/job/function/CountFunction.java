package org.qingchao.flink.job.function;

import com.alibaba.fastjson.JSON;
import com.dianping.cat.Cat;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.qingchao.flink.job.config.FlinkConfigDto;
import org.qingchao.flink.job.repo.ClientFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.qingchao.flink.job.constant.Constant.*;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-02 2:07 下午
 */
@Slf4j
public class CountFunction extends AbstractFunction implements IFunction {

    private String groupByField;

    public CountFunction(FlinkConfigDto config) {
        super(config);
        this.groupByField = config.getAggConfig().getString(GROUP_BY_FIELD);
    }

    @Override
    public void add_1min(Map<String, Object> value, Map<String, Object> accumulator) {
        Map<String, Object> middleFeature_1min = (Map) accumulator.get(MIDDLE_FEATURE_1MIN);

        //根据是否分组生成不同的key
        String key = featureNamePrefix;
        if (StringUtils.isNotBlank(groupByField)) {
            final Object groupByKey = AviatorEvaluator.execute(groupByField, value, true);
            if (Objects.isNull(groupByKey)) {
                return;
            }
            key = featureNamePrefix + "_" + groupByKey.toString();
        }

        Map<String, Long> middleFeature = (Map<String, Long>) middleFeature_1min.getOrDefault(featureNamePrefix, new HashMap<String, Long>());
        {
            Long count = middleFeature.getOrDefault(key, 0L);
            count++;
            middleFeature.put(key, count);
        }

        middleFeature_1min.put(featureNamePrefix, middleFeature);
    }

    @Override
    public void merge_1min(Map<String, Object> a, Map<String, Object> b) {
        //from a
        //1. a中无中间结果
        Map<String, Object> middleFeature_1minA = (Map) a.get(MIDDLE_FEATURE_1MIN);
        if (!middleFeature_1minA.containsKey(featureNamePrefix)) {
            return;
        }
        Map<String, Long> middleFeatureA = (Map<String, Long>) middleFeature_1minA.get(featureNamePrefix);

        //2. b中无中间结果
        Map<String, Object> middleFeature_1minB = (Map) b.get(MIDDLE_FEATURE_1MIN);
        if (!middleFeature_1minB.containsKey(featureNamePrefix)) {
            middleFeature_1minB.put(featureNamePrefix, middleFeatureA);
            return;
        }

        //3. a、b中都包含中间结果
        Map<String, Long> middleFeatureB = (Map<String, Long>) middleFeature_1minB.get(featureNamePrefix);
        middleFeatureA.forEach((key, value) -> {
            Long countB = middleFeatureB.getOrDefault(key, 0L);
            countB += value;
            middleFeatureB.put(key, countB);
        });
    }

    @Override
    public void kv_1min(Map<String, Object> value) {
        try (Jedis jedis = ClientFactory.getFlinkJedisClient().getResource();) {
            //1.检查是否存在对应的中间结果
            Map<String, Object> middleFeature_1min = (Map<String, Object>) value.get(MIDDLE_FEATURE_1MIN);
            if (!middleFeature_1min.containsKey(featureNamePrefix)) {
                return;
            }

            //2.拿到中间结果，并保存到redis中，zset的value为 毫秒值_middleFeature
            Map<String, Long> middleFeature = (Map<String, Long>) middleFeature_1min.get(featureNamePrefix);
            final String middleFeatureStr = JSON.toJSONString(middleFeature);
            final long startScore = System.currentTimeMillis();
            String _id = (String) value.get(_ID);
            final String zSetKey = featureNamePrefix + _id;
            final String member = startScore + "_" + middleFeatureStr;

            try (final Pipeline pipelined = jedis.pipelined();) {
                pipelined.zadd(zSetKey, startScore, member);
                pipelined.pexpire(zSetKey, this.expireMillis);
                pipelined.sync();
            } catch (Throwable throwable) {
                final String format = String.format("CountFunction.kv_1min, jedis pipeline error, error:%s", throwable);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }

            //3.拿到历史中间结果，并计算最终特征
            final Map<String, Object> kv = (Map<String, Object>) value.getOrDefault(KV, new HashMap<String, Object>());
            //3.1.相对时间窗口特征计算
            relativeWindows_1min.forEach((window, config) -> {
                final Map<String, Long> feature = aggregateFeature(jedis, zSetKey, startScore - window, startScore);
                feature.forEach((k, v) -> {
                    kv.put(k + "_" + config, v);
                });
            });
            //3.2.整点时间窗口特征计算
            wholeWindows_1min.forEach(window -> {
                final Tuple2<Long, Long> wholeWindow = getWholeWindow(window);
                final Map<String, Long> feature = aggregateFeature(jedis, zSetKey, wholeWindow.f0, wholeWindow.f1);
                feature.forEach((k, v) -> {
                    kv.put(k + "_" + window, v);
                });
            });

            //4.删除redis中聚合窗口以外的key、value
            jedis.zremrangeByScore(zSetKey, 0, startScore - this.expireMillis);

            value.put(KV, kv);
        } catch (Throwable throwable) {
            final String format = String.format("CountFunction error, value:%s, error:%s", value, throwable);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }

    private Map<String, Long> aggregateFeature(Jedis jedis, String key, Long start, Long end) {
        //redis zrange条件为 start < members <= end
        final Set<String> zrange = jedis.zrangeByScore(key, start, end);

        Map<String, Long> feature = new HashMap<>();
        zrange.stream().map(item -> item.substring(item.indexOf("_") + 1))
                .map(item -> (Map<String, Long>) JSON.parseObject(item, MAP_TYPE_REFERENCE))
                .forEach(item -> {
                    Map<String, Long> map = (Map<String, Long>) item;
                    map.forEach((k, v) -> {
                        final Long count = feature.getOrDefault(k, 0L);
                        feature.put(k, count + v);
                    });
                });

        return feature;
    }
}
