package org.qingchao.flink.job.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dianping.cat.Cat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.util.Bytes;
import org.qingchao.flink.job.base.ServiceBase;
import org.qingchao.flink.job.repo.AliHBaseRepo;
import org.qingchao.flink.job.repo.ClientFactory;
import org.qingchao.flink.job.repo.kv.RawKeyValue;
import org.qingchao.flink.job.repo.kv.RawRow;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.qingchao.flink.job.constant.Constant.*;
import static org.qingchao.flink.job.streamfunction.ResourceInitRichMapFunction.getFeatureBiztypeMap;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-11 2:58 下午
 */
@Slf4j
public class HBaseSink {

    public static void sink(Iterable<Map<String, Object>> elements) {
        final AliHBaseRepo aliHBaseRepo = ClientFactory.getAliHBaseRepo();
        final Map<String, JSONObject> sinkMap = SinkFactory.createHbaseSink();
        Map<String, List<RawRow>> batchSinks = new ConcurrentHashMap<>();
        //profile redis 批量更新
        Map<Tuple2<String, String>, Map<String, String>> batchUpdateProfileRedis = new HashMap<>();
        Map<String, Map<String, Object>> send2Kafka = new HashMap<>();
        elements.forEach(value -> {
            if (!value.containsKey(TS)) {
                value.put(TS, System.currentTimeMillis());
            }
            final Long ts = (Long) value.get(TS);
            Map<String, Object> featureKv = (Map<String, Object>) value.get(KV);
            if (Objects.nonNull(featureKv) && featureKv.size() > 0) {
                featureKv.forEach((String featureName, Object featureValue) -> {
                    final JSONObject jsonObject = sinkMap.get(featureName);
                    if (Objects.nonNull(jsonObject)) {
                        final String table = jsonObject.getString("table");
                        final long ttl = jsonObject.getLongValue("ttl");
                        final String rename = jsonObject.getString("rename");
                        RawRow rawRow = new RawRow();
                        if (ttl > 0) {
                            rawRow.setTtl(ttl * 1000);
                        }
                        String _id = (String) value.get(_ID);
                        rawRow.setRowKey(rowKey(_id));
                        final byte[] key = Bytes.toBytes(rename);
                        final String sValue = ((Map<String, Object>) value.get(KV)).get(featureName).toString();
                        final byte[] bValue = Bytes.toBytes(sValue);
                        List<RawKeyValue> columns = new ArrayList<>();
                        columns.add(new RawKeyValue(key, ts, bValue));
                        rawRow.setColumns(columns);
                        final List<RawRow> tableRowList = batchSinks.getOrDefault(table, new LinkedList<>());
                        tableRowList.add(rawRow);
                        batchSinks.put(table, tableRowList);
                        log.info("Add data to batchSinks, table:{}, id:{}, column:{}, value:{}", table, _id, rename, sValue);

                        //更新profile redis 缓存
                        final Tuple2<String, String> redisKey = Tuple2.of(_id, table);
                        final Map<String, String> updateMap = batchUpdateProfileRedis.getOrDefault(redisKey, new HashMap<>());
                        updateMap.put(rename, combineCacheValue(sValue));
                        batchUpdateProfileRedis.put(redisKey, updateMap);

                        //发送数据到kafka
                        final String bizType = getFeatureBiztypeMap().get(rename);
                        if (StringUtils.isNotBlank(bizType)) {
                            final Map<String, Object> data = send2Kafka.getOrDefault(bizType, new HashMap<>());
                            data.put(ID, _id);
                            data.put(DATA_TYPE, value.get(_ID_TYPE));
                            data.put(TS, ts);
                            data.put(BIZ_TYPE, bizType);
                            final Map<String, Object> kv = (Map<String, Object>) data.getOrDefault(KV, new HashMap<>());
                            kv.put(rename, sValue);
                            data.put(KV, kv);
                            send2Kafka.put(bizType, data);
                        }

                        //cat 打点监控
                        Map<String, String> tagMetric = new HashMap<>();
                        tagMetric.put(metric, rename);
                        Cat.logMetricForCount(FLINK_OUTPUT, tagMetric);
                    }
                });
            }
        });

        //更新hbase
        batchSinks.forEach((k, v) -> {
            final boolean put =
                    ServiceBase.withTransaction("HBaseSink", k, () -> aliHBaseRepo.put(v, k), false);
            log.info("--------batch insert hbase，table:{}, result:{}", k, put);
        });

        //更新profile redis
        try (final Jedis jedis = ClientFactory.getProfileJedisClient().getResource()) {
            batchUpdateProfileRedis.forEach((k, v) -> {
                //1.删除kv接口的redis缓存
                final String kvCacheKey = getKvCacheKey(rowKey(k.f0), k.f1);
                jedis.del(kvCacheKey);

                //2.更新特征组的redis缓存
                final String featureGroupCacheKey = getFeatureGroupCacheKey(k.f0, k.f1);
                if (jedis.exists(featureGroupCacheKey)) {
                    ServiceBase.withTransaction("ProfileRedisUpdate", k.f1,
                            () -> {
                                //如果缓存存在，且存活时间大于10s，则更新；否则，删除缓存
                                final Long ttl = jedis.ttl(featureGroupCacheKey);
                                if (ttl < 10) {
                                    jedis.del(featureGroupCacheKey);
                                } else {
                                    final Transaction tx = jedis.multi();
                                    {
                                        tx.hmset(featureGroupCacheKey, v);
                                        tx.expire(featureGroupCacheKey, ttl.intValue());
                                        tx.exec();
                                    }
                                }
                                return true;
                            },
                            false);
                    log.info("update profileRedis, id:{}, table:{}", k.f0, k.f1);
                    log.debug("update profileRedis, id:{}, table:{}, value:{}", k.f0, k.f1, v);
                }
            });
            log.info("update profile redis ok!");
        } catch (Throwable throwable) {
            final String format = String.format("update profile redis error, error:%s", throwable);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }

        //特征数据发默认kafka topic
        try {
            send2Kafka.forEach((k, v) -> {
                final String id = (String) v.get(ID);
                final String data = JSON.toJSONString(v);
                KafkaSink.sink(YPP_PROFILE_OUT, id, data);
                log.info("send to kafka, id:{}, topic:{}, data:{}", id, YPP_PROFILE_OUT, data);
            });
        } catch (Throwable throwable) {
            final String format = String.format("send data to kafka error, error:%s", throwable);
            log.error(format, throwable);
            Cat.logError(format, throwable);
        }
    }

    private static String rowKey(String id) {
        return String.format("%c%s", id.charAt(id.length() - 1), id);
    }

    private static String getFeatureGroupCacheKey(String key, String table) {
        return String.format("group:%s:%s", table, key);
    }

    public static String getKvCacheKey(String rowKey, String table) {
        final String parse = String.format("%s@%s", table, "");
        int version = 1;
        return String.format("%s:%d-%s:%s", "gzip", version, parse, rowKey);
    }

    private static String combineCacheValue(String value) {
        return String.format("ts:%d,%s", System.currentTimeMillis(), value);
    }

}
