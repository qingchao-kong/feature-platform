package org.qingchao.flink.job.function;

import com.dianping.cat.Cat;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
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
 * @date 2020/12/18
 */
@Slf4j
public class MinFunction extends AbstractFunction implements IFunction {
    private String fieldType;
    private String field;

    public MinFunction(FlinkConfigDto config) {
        super(config);
        this.fieldType = config.getAggConfig().getString(FIELD_TYPE);
        this.field = config.getAggConfig().getString(FIELD);
    }

    /**
     * 每个window每个id第二条数据及后面数据的处理逻辑
     *
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public void add_1min(Map<String, Object> value, Map<String, Object> accumulator) {
        final Object execute = AviatorEvaluator.execute(field, value, true);
        if (Objects.isNull(execute)) {
            return;
        }
        Map<String, Object> middleFeature_1min = (Map) accumulator.get(MIDDLE_FEATURE_1MIN);
        if (DOUBLE_FLAG.equals(fieldType)) {
            double currentValueDouble = NumberUtils.toDouble(execute.toString());
            Double accValue = (Double) middleFeature_1min.getOrDefault(featureNamePrefix, DOUBLE_POSITIVE_INFINITY);
            double minValue = Math.min(currentValueDouble, accValue);
            middleFeature_1min.put(featureNamePrefix, minValue);
        }
        if (LONG_FLAG.equals(fieldType)) {
            long currentValueLong = NumberUtils.toLong(execute.toString());
            Long accValue = (Long) middleFeature_1min.getOrDefault(featureNamePrefix, LONG_POSITIVE_INFINITY);
            long minValue = Math.min(currentValueLong, accValue);
            middleFeature_1min.put(featureNamePrefix, minValue);
        }
    }

    /**
     * 各subTask内部处理后，reduce逻辑
     *
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public void merge_1min(Map<String, Object> value, Map<String, Object> accumulator) {
        //1.value中不包含
        Map<String, Object> valueFeature_1min = (Map) value.get(MIDDLE_FEATURE_1MIN);
        if (!valueFeature_1min.containsKey(featureNamePrefix)) {
            return;
        }

        //2.acc中不包含
        Map<String, Object> middleFeature_1min = (Map) accumulator.get(MIDDLE_FEATURE_1MIN);
        if (!middleFeature_1min.containsKey(featureNamePrefix)) {
            middleFeature_1min.put(featureNamePrefix, valueFeature_1min.get(featureNamePrefix));
        }

        //3.value、acc都包含
        final Object currentValue = valueFeature_1min.get(featureNamePrefix);
        final Object accValue = middleFeature_1min.get(featureNamePrefix);

        if (DOUBLE_FLAG.equals(fieldType)) {
            double maxValue = Math.min((Double) currentValue, (Double) accValue);
            middleFeature_1min.put(featureNamePrefix, maxValue);
        }
        if (LONG_FLAG.equals(fieldType)) {
            long maxValue = Math.min((Long) currentValue, (Long) accValue);
            middleFeature_1min.put(featureNamePrefix, maxValue);
        }
    }

    /**
     * 获取所有中间结果，保存此次window得到的中间结果，计算最终特征
     *
     * @param value
     */
    @Override
    public void kv_1min(Map<String, Object> value) {
        try (final Jedis jedis = ClientFactory.getFlinkJedisClient().getResource()) {
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
                final String format = String.format("MinFunction.kv_1min, jedis pipeline error, error:%s", throwable);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }

            //3.拿到历史中间结果，并计算最终特征
            final Map<String, Object> kv = (Map<String, Object>) value.getOrDefault("kv", new HashMap<String, Object>());
            relativeWindows_1min.forEach((window, config) -> {
                //redis zrange条件为 start < members <= end
                final Set<String> zrange = jedis.zrangeByScore(zSetKey, startScore - window, startScore);
                if (LONG_FLAG.equals(fieldType)) {
                    final long min = zrange.stream().mapToLong(item -> {
                        final String s = item.substring(item.indexOf("_") + 1);
                        return Long.parseLong(s);
                    }).min().getAsLong();
                    kv.put(featureNamePrefix + "_" + config, min);
                } else if (DOUBLE_FLAG.equals(fieldType)) {
                    final double min = zrange.stream().mapToDouble(item -> {
                        final String s = item.substring(item.indexOf("_") + 1);
                        return Double.parseDouble(s);
                    }).min().getAsDouble();
                    kv.put(featureNamePrefix + "_" + config, min);
                }
            });

            //4.删除redis中聚合窗口以外的key、value
            jedis.zremrangeByScore(zSetKey, 0, startScore - this.expireMillis);

            value.put(KV, kv);
        } catch (Throwable throwable) {
            final String format = String.format("MinFunction error, value:%s, error:%s", value, throwable);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }
}
