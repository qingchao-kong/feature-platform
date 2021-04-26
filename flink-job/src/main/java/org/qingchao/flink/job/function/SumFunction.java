package org.qingchao.flink.job.function;

import com.dianping.cat.Cat;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
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
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/11 2:02 下午
 */
@Slf4j
public class SumFunction extends AbstractFunction implements IFunction {
    private String fieldType;
    private String field;

    public SumFunction(FlinkConfigDto config) {
        super(config);
        this.fieldType = config.getAggConfig().getString(FIELD_TYPE);
        this.field = config.getAggConfig().getString(FIELD);
    }

    @Override
    public void add_1min(Map<String, Object> value, Map<String, Object> accumulator) {
        final Object execute = AviatorEvaluator.execute(field, value, true);
        if (Objects.isNull(execute)) {
            return;
        }
        Map<String, Object> middleFeature_1min = (Map) accumulator.get(MIDDLE_FEATURE_1MIN);
        if (DOUBLE_FLAG.equals(fieldType)) {
            Double sum = (Double) middleFeature_1min.getOrDefault(featureNamePrefix, new Double(0));
            sum = sum + Double.parseDouble(execute.toString());
            middleFeature_1min.put(featureNamePrefix, sum);
        }
        if (LONG_FLAG.equals(fieldType)) {
            long sum = (long) middleFeature_1min.getOrDefault(featureNamePrefix, new Long(0));
            sum = sum + Long.parseLong(execute.toString());
            middleFeature_1min.put(featureNamePrefix, sum);
        }
    }

    @Override
    public void merge_1min(Map<String, Object> a, Map<String, Object> b) {
        //1.a中不包含
        Map<String, Object> middleFeature_1minA = (Map) a.get(MIDDLE_FEATURE_1MIN);
        if (!middleFeature_1minA.containsKey(featureNamePrefix)) {
            return;
        }

        //2.b中不包含
        Map<String, Object> middleFeature_1minB = (Map) b.get(MIDDLE_FEATURE_1MIN);
        if (!middleFeature_1minB.containsKey(featureNamePrefix)) {
            middleFeature_1minB.put(featureNamePrefix, middleFeature_1minA.get(featureNamePrefix));
        }

        //3.a、b中都包含
        final Object aValue = middleFeature_1minA.get(featureNamePrefix);
        final Object bValue = middleFeature_1minB.get(featureNamePrefix);
        if (LONG_FLAG.equals(fieldType)) {
            Long count = (Long) aValue + (Long) bValue;
            middleFeature_1minB.put(featureNamePrefix, count);
        }
        if (DOUBLE_FLAG.equals(fieldType)) {
            Double count = (Double) aValue + (Double) bValue;
            middleFeature_1minB.put(featureNamePrefix, count);
        }
    }

    @Override
    public void kv_1min(Map<String, Object> value) {
        try (Jedis jedis = ClientFactory.getFlinkJedisClient().getResource();) {
            //1.检查是否存在对应的中间结果
            Map<String, Object> middleFeature1Min = (Map<String, Object>) value.get(MIDDLE_FEATURE_1MIN);
            if (!middleFeature1Min.containsKey(featureNamePrefix)) {
                return;
            }

            //2.拿到中间结果，并保存到redis中，zset的value为 毫秒值_middleFeature
            Object middleFeature = middleFeature1Min.get(featureNamePrefix);
            final long startScore = System.currentTimeMillis();
            String _id = (String) value.get(_ID);
            final String zSetKey = featureNamePrefix + _id;
            final String member = startScore + "_" + middleFeature;

            try (final Pipeline pipelined = jedis.pipelined();) {
                pipelined.zadd(zSetKey, startScore, member);
                pipelined.pexpire(zSetKey, this.expireMillis);
                pipelined.sync();
            } catch (Throwable throwable) {
                final String format = String.format("SumFunction.kv_1min, jedis pipeline error, error:%s", throwable);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }

            //3.拿到历史中间结果，并计算最终特征
            final Map<String, Object> kv = (Map<String, Object>) value.getOrDefault(KV, new HashMap<String, Object>());
            relativeWindows_1min.forEach((window, config) -> {
                //redis zrange条件为 start < members <= end
                final Set<String> zrange = jedis.zrangeByScore(zSetKey, startScore - window, startScore);
                if (LONG_FLAG.equals(fieldType)) {
                    final long sum = zrange.stream().mapToLong(item -> {
                        final String s = item.substring(item.indexOf("_") + 1);
                        return Long.parseLong(s);
                    }).sum();
                    kv.put(featureNamePrefix + "_" + config, sum);
                } else if (DOUBLE_FLAG.equals(fieldType)) {
                    final double sum = zrange.stream().mapToDouble(item -> {
                        final String s = item.substring(item.indexOf("_") + 1);
                        return Double.parseDouble(s);
                    }).sum();
                    kv.put(featureNamePrefix + "_" + config, sum);
                }
            });

            //4.删除redis中聚合窗口以外的key、value
            jedis.zremrangeByScore(zSetKey, 0, startScore - this.expireMillis);

            value.put(KV, kv);
        } catch (Throwable throwable) {
            final String format = String.format("SumFunction error, value:%s, error:%s", value, throwable);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }
}
