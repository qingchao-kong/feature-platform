package org.qingchao.flink.job.function;

import com.alibaba.fastjson.JSON;
import com.dianping.cat.Cat;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.qingchao.flink.job.config.FlinkConfigDto;
import org.qingchao.flink.job.repo.ClientFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.stream.Collectors;

import static org.qingchao.flink.job.constant.Constant.*;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-01-09 4:46 下午
 */
@Slf4j
public class SetFunction extends AbstractFunction {

    private String field;
    private String eventTimeField = "ext.eventTime";
    private Integer capacity;
    private Boolean needTs;

    public SetFunction(FlinkConfigDto config) {
        super(config);
        this.field = config.getAggConfig().getString(FIELD);
        this.eventTimeField = config.getAggConfig().getString(EVENT_TIME_FIELD);
        this.capacity = config.getAggConfig().getInteger(CAPACITY);
        this.needTs = config.getAggConfig().getBoolean(NEED_TS);
    }

    @Override
    public void add_1min(Map<String, Object> value, Map<String, Object> accumulator) {
        final Object fieldExecute = AviatorEvaluator.execute(field, value, true);
        final Object eventTimeExecute = AviatorEvaluator.execute(eventTimeField, value, true);
        //value中值为null时或时间戳为null时过滤，其他情况不过滤
        if (Objects.isNull(fieldExecute) || Objects.isNull(eventTimeExecute)) {
            return;
        }
        final String fieldValue = fieldExecute.toString();
        final long eventTimeValue = Long.parseLong(eventTimeExecute.toString());

        Map<String, Object> middleFeature_1min = (Map) accumulator.get(MIDDLE_FEATURE_1MIN);
        if (!middleFeature_1min.containsKey(featureNamePrefix)) {
            middleFeature_1min.put(featureNamePrefix, new HashMap<String, Long>());
        }

        final Map<String, Long> featureMap = (Map<String, Long>) middleFeature_1min.get(featureNamePrefix);
        featureMap.put(fieldValue + "|" + eventTimeValue, eventTimeValue);
    }

    @Override
    public void merge_1min(Map<String, Object> aAcc, Map<String, Object> bAcc) {
        Map<String, Object> aMiddleFeature_1min = (Map) aAcc.get(MIDDLE_FEATURE_1MIN);
        Map<String, Object> bMiddleFeature_1min = (Map) bAcc.get(MIDDLE_FEATURE_1MIN);

        final Map<String, Long> aFeatureMap = (Map<String, Long>) aMiddleFeature_1min.getOrDefault(featureNamePrefix, new HashMap<String, Long>());
        Map<String, Long> bFeatureMap = (Map<String, Long>) bMiddleFeature_1min.getOrDefault(featureNamePrefix, new HashMap<String, Long>());

        aFeatureMap.forEach((k, v) -> {
            if (!(bFeatureMap.containsKey(k) && bFeatureMap.get(k) > v)) {
                bFeatureMap.put(k, v);
            }
        });

        Map<String, Long> map;
        if (bFeatureMap.size() > capacity) {
            map = sortByValue(bFeatureMap, true, capacity);
        }
        {
            map = bFeatureMap;
        }

        if (map.size() > 0) {
            bMiddleFeature_1min.put(featureNamePrefix, map);
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
            Map<String, Long> middleFeature = (Map<String, Long>) middleFeature1Min.get(featureNamePrefix);
            final Map<String, Long> sortedMiddleFeatureMap = sortByValue(middleFeature, true, capacity);
            final String featureJsonStr = JSON.toJSONString(sortedMiddleFeatureMap);

            final long startScore = System.currentTimeMillis();
            String _id = (String) value.get(_ID);
            final String zSetKey = featureNamePrefix + _id;
            final String member = startScore + "_" + featureJsonStr;

            try (final Pipeline pipelined = jedis.pipelined();) {
                pipelined.zadd(zSetKey, startScore, member);
                pipelined.pexpire(zSetKey, this.expireMillis);
                pipelined.sync();
            } catch (Throwable throwable) {
                final String format = String.format("SetFunction.kv_1min, jedis pipeline error, error:%s", throwable);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }

            //3.拿到历史中间结果，并计算最终特征
            final Map<String, Object> kv = (Map<String, Object>) value.getOrDefault(KV, new HashMap<String, Object>());
            relativeWindows_1min.forEach((window, config) -> {
                final Set<String> zrange = jedis.zrevrangeByScore(zSetKey, startScore, startScore - window);
                Map<String, Long> map = Maps.newLinkedHashMap();
                //得到由近到远的中间结果map，map的key为value|eventTime，value为eventTime
                final List<Map<String, Long>> collect = zrange.stream()
                        .map(str -> str.substring(str.indexOf("_") + 1))
                        .map(str -> (Map<String, Long>) JSON.parseObject(str, Map.class))
                        .collect(Collectors.toList());
                for (Map<String, Long> item : collect) {
                    item.forEach((k, v) -> {
                        final String fieldValue = k.substring(0, k.indexOf("|"));
                        if (!map.containsKey(fieldValue) || map.get(fieldValue) < v) {
                            map.put(fieldValue, v);
                        }
                    });
                    if (map.size() >= capacity) {
                        break;
                    }
                }

                //截断并排序
                Map<String, Long> featureMap = sortByValue(map, true, capacity);

                //获取队列
                final List<String> list = featureMap.entrySet().stream()
                        .map(entry -> needTs ? entry.getKey() + "|" + entry.getValue() : entry.getKey())
                        .collect(Collectors.toList());

                kv.put(featureNamePrefix + "_" + config, JSON.toJSONString(list));
            });
            //4.删除redis中聚合窗口以外的key、value
            jedis.zremrangeByScore(zSetKey, 0, startScore - this.expireMillis);

            value.put(KV, kv);
        } catch (Throwable throwable) {
            final String format = String.format("SetFunction error, value:%s, error:%s", value, throwable);
            log.error(format, throwable);
            log.error(format, throwable);
        }
    }
}
