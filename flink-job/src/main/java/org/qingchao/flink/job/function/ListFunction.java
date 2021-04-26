package org.qingchao.flink.job.function;

import com.alibaba.fastjson.JSON;
import com.dianping.cat.Cat;
import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.qingchao.flink.job.config.FlinkConfigDto;
import org.qingchao.flink.job.repo.ClientFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.stream.Collectors;

import static org.qingchao.flink.job.constant.Constant.*;

/**
 * @author kongqingchao
 * @date 2021/1/8
 */
@Slf4j
public class ListFunction extends AbstractFunction implements IFunction {

    private String field;
    private String eventTimeField = "ext.eventTime";
    private Integer capacity;
    private Boolean needTs;

    public ListFunction(FlinkConfigDto config) {
        super(config);
        this.field = config.getAggConfig().getString(FIELD);
        this.eventTimeField = config.getAggConfig().getString(EVENT_TIME_FIELD);
        this.capacity = config.getAggConfig().getInteger(CAPACITY);
        this.needTs = config.getAggConfig().getBoolean(NEED_TS);
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

    /**
     * 各subTask内部处理后，reduce逻辑
     *
     * @param aAcc
     * @param bAcc
     * @return
     */
    @Override
    public void merge_1min(Map<String, Object> aAcc, Map<String, Object> bAcc) {
        Map<String, Object> aMiddleFeature_1min = (Map) aAcc.get(MIDDLE_FEATURE_1MIN);
        Map<String, Object> bMiddleFeature_1min = (Map) bAcc.get(MIDDLE_FEATURE_1MIN);

        final Map<String, Long> aFeatureMap = (Map<String, Long>) aMiddleFeature_1min.getOrDefault(featureNamePrefix, new HashMap<String, Long>());
        Map<String, Long> bFeatureMap = (Map<String, Long>) bMiddleFeature_1min.getOrDefault(featureNamePrefix, new HashMap<String, Long>());
        bFeatureMap.putAll(aFeatureMap);

        if (bFeatureMap.size() > capacity) {
            bFeatureMap = sortByValue(bFeatureMap, true, capacity);
        }

        if (bFeatureMap.size() > 0) {
            bMiddleFeature_1min.put(featureNamePrefix, bFeatureMap);
        }
    }

    /**
     * 获取所有中间结果，保存此次window得到的中间结果，计算最终特征
     *
     * @param value
     */
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
            final List<Tuple2<String, Long>> listMiddleFeature = sortedMiddleFeatureMap.entrySet().stream().map(en -> Tuple2.of(en.getKey(), en.getValue())).collect(Collectors.toList());
            final String featureJsonStr = JSON.toJSONString(listMiddleFeature);

            final long startScore = System.currentTimeMillis();
            String _id = (String) value.get(_ID);
            final String zSetKey = featureNamePrefix + _id;
            final String member = startScore + "_" + featureJsonStr;

            try (final Pipeline pipelined = jedis.pipelined();) {
                pipelined.zadd(zSetKey, startScore, member);
                pipelined.pexpire(zSetKey, this.expireMillis);
                pipelined.sync();
            } catch (Throwable throwable) {
                final String format = String.format("ListFunction.kv_1min, jedis pipeline error, error:%s", throwable);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }

            //3.拿到历史中间结果，并计算最终特征
            final Map<String, Object> kv = (Map<String, Object>) value.getOrDefault(KV, new HashMap<String, Object>());
            relativeWindows_1min.forEach((window, config) -> {
                final Set<String> zrange = jedis.zrevrangeByScore(zSetKey, startScore, startScore - window);

                final List<Tuple2<String, Long>> allMiddleFreature = new LinkedList<>();
                zrange.stream()
                        .map(str -> str.substring(str.indexOf("_") + 1))
                        .forEach(str -> {
                            final List<Tuple2<String, Long>> list = (List<Tuple2<String, Long>>) JSON.parseObject(str, TUPLE_LIST_TYPE_REFERENCE);
                            allMiddleFreature.addAll(list);
                        });

                //截断；已经是有序的，截断即可
                final int min = Math.min(allMiddleFreature.size(), capacity);
                List<String> subList = allMiddleFreature.subList(0, min).stream()
                        .map(tuple2 -> tuple2.f0)
                        .map(str -> needTs ? str : str.substring(0, str.indexOf("|")))
                        .collect(Collectors.toList());

                kv.put(featureNamePrefix + "_" + config, JSON.toJSONString(subList));
            });

            //4.删除redis中聚合窗口以外的key、value
            jedis.zremrangeByScore(zSetKey, 0, startScore - this.expireMillis);

            value.put(KV, kv);
        } catch (Throwable throwable) {
            final String format = String.format("ListFunction error, value:%s, error:%s", value, throwable);
            log.error(format, throwable);
            log.error(format, throwable);
        }
    }
}