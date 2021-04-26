package org.qingchao.flink.job.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.dianping.cat.Cat;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.qingchao.flink.job.config.FlinkConfigDto;

import java.util.*;

import static org.qingchao.flink.job.constant.Constant.*;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-19 3:58 下午
 */
@Slf4j
@Data
public abstract class AbstractFunction implements IFunction {
    protected static final String GROUP_BY_FIELD = "groupByField";
    protected static final String FIELD = "field";
    protected static final String FIELD_TYPE = "fieldType";
    protected static final String EVENT_TIME = "eventTime";
    protected static final String EVENT_TIME_FIELD = "eventTimeField";
    protected static final String CAPACITY = "capacity";
    protected static final String NEED_TS = "needTs";
    protected static final String MINUTE_FLAG = "m";
    protected static final String HOUR_FLAG = "h";
    protected static final String LONG_FLAG = "long";
    protected static final String DOUBLE_FLAG = "double";
    /**
     * 整点窗口
     */
    protected static final String WHOLE_WINDOW = "wholeWindow";
    /**
     * 相对窗口
     */
    protected static final String RELATIVE_WINDOW = "relativeWindow";

    protected static final long LONG_POSITIVE_INFINITY = Long.MAX_VALUE;
    protected static final long LONG_NEGATIVE_INFINITY = -Long.MAX_VALUE;
    protected static final double DOUBLE_POSITIVE_INFINITY = Double.POSITIVE_INFINITY;
    protected static final double DOUBLE_NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    public static final TypeReference TUPLE_LIST_TYPE_REFERENCE = new TypeReference<List<Tuple2<String, Long>>>() {
    };
    public static final TypeReference MAP_TYPE_REFERENCE = new TypeReference<Map<String, Long>>() {
    };

    protected String type;
    protected FlinkConfigDto config;
    protected String featureNamePrefix;

    /**
     * 相对时间窗口
     */
    protected Map<Long, String> relativeWindows_1min = new HashMap<>();
    protected Map<Long, String> relativeWindows_1h = new HashMap<>();

    /**
     * 整点时间窗口
     */
    protected Set<String> wholeWindows_1min = new HashSet<>();
    protected Set<String> wholeWindows_1h = new HashSet<>();

    /**
     * redis中间结果过期时间，默认1h过期时间；单位毫秒
     */
    protected Long expireMillis = 60 * 60 * 1000L;

    public AbstractFunction(FlinkConfigDto config) {
        this.type = config.getConfigType();
        this.config = config;
        this.featureNamePrefix = config.getFeatureNamePrefix();
        for (Object item : config.getWindows()) {
            JSONObject windowConfig = (JSONObject) item;
            final String delay = windowConfig.getString(DELAY);
            final String window = windowConfig.getString(WINDOW);
            //窗口类型，相对时间窗口、绝对时间窗口
            final String windowType = windowConfig.getString(WINDOW_TYPE);

            if (WHOLE_WINDOW.equals(windowType)) {
                wholeWindows_1min.add(window);
                switch (window) {
                    case "today":
                        this.expireMillis = Math.max(this.expireMillis, 24 * 60 * 60 * 1000L);
                        break;
                    case "yesterday":
                        this.expireMillis = Math.max(this.expireMillis, 48 * 60 * 60 * 1000L);
                        break;
                }
            } else {
                final Long windowMillis = getWindowMillis(window);
                //更新中间结果过期时间
                this.expireMillis = Math.max(this.expireMillis, windowMillis);
                if (windowMillis > 0) {
                    if (delay.contains(MINUTE_FLAG)) {
                        relativeWindows_1min.put(windowMillis, window);
                    } else {
                        if (delay.contains(HOUR_FLAG)) {
                            relativeWindows_1h.put(windowMillis, window);
                        }
                    }
                }
            }
        }
    }

    private Long getWindowMillis(String window) {
        final String substring = window.substring(0, window.length() - 1);
        if (window.contains(MINUTE_FLAG)) {
            return Long.parseLong(substring) * 60 * 1000;
        } else if (window.contains(HOUR_FLAG)) {
            return Long.parseLong(substring) * 60 * 60 * 1000;
        } else {
            final String format = String.format("window format error, window:%s", window);
            log.error(format);
            Cat.logError(format, new Exception(format));
            return 0L;
        }
    }

    /**
     * Sort map by value
     *
     * @param map    map source
     * @param isDesc 是否降序，true：降序，false：升序
     * @param limit  取前几条
     * @return 已排序map
     */
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean isDesc, int limit) {
        Map<K, V> result = Maps.newLinkedHashMap();
        if (isDesc) {
            map.entrySet().stream()
                    .sorted(Map.Entry.<K, V>comparingByValue().reversed())
                    .limit(limit)
                    .forEach(e -> result.put(e.getKey(), e.getValue()));
        } else {
            map.entrySet().stream()
                    .sorted(Map.Entry.<K, V>comparingByValue())
                    .limit(limit)
                    .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        }
        return result;
    }

    /**
     * Sort map by key
     *
     * @param map    待排序的map
     * @param isDesc 是否降序，true：降序，false：升序
     * @param limit  取前几条
     * @return 已排序map
     */
    public static <K extends Comparable<? super K>, V> Map<K, V> sortByKey(Map<K, V> map, boolean isDesc, int limit) {
        Map<K, V> result = Maps.newLinkedHashMap();
        if (isDesc) {
            map.entrySet().stream()
                    .sorted(Map.Entry.<K, V>comparingByKey().reversed())
                    .limit(limit)
                    .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        } else {
            map.entrySet().stream()
                    .sorted(Map.Entry.<K, V>comparingByKey())
                    .limit(limit)
                    .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
        }
        return result;
    }

    protected static Tuple2<Long, Long> getWholeWindow(String window) {
        Long current = System.currentTimeMillis();  //当前时间的时间戳
        long todayZero = current / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset();
        switch (window) {
            case "today":
                return Tuple2.of(todayZero, current);
            case "yesterday":
                final long yesterdayZero = todayZero - 24 * 60 * 60 * 1000;
                return Tuple2.of(yesterdayZero, todayZero);
            default:
                return null;
        }
    }
}
