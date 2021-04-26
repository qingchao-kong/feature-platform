package org.qingchao.flink.job.streamfunction;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.dianping.cat.Cat;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.qingchao.flink.job.config.ApolloConfigService;
import org.qingchao.flink.job.config.FlinkConfigDto;
import org.qingchao.flink.job.config.Response;
import org.qingchao.flink.job.function.AtomFunction;
import org.qingchao.flink.job.function.FunctionFactory;
import org.qingchao.flink.job.function.IFunction;
import org.qingchao.flink.job.sink.SinkFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.qingchao.flink.job.constant.Constant.*;

/**
 * 描述:资源初始化 RichMapFunction
 *
 * @author kongqingchao
 * @create 2020-12-18 2:05 下午
 */
@Slf4j
public class ResourceInitRichMapFunction extends RichMapFunction<Map<String, Object>, Map<String, Object>> {
    public static LocalDateTime localDateTime = LocalDateTime.of(1999, 1, 1, 1, 1, 1);

    /**
     * 1s延迟的特征聚合配置
     */
    private static Map<String, List<IFunction>> FUNCTIONS_MAP_1S = new HashMap<>();

    /**
     * 1min延迟的特征聚合配置
     */
    private static Map<String, List<IFunction>> FUNCTIONS_MAP_1MIN = new HashMap<>();

    /**
     * 1h延迟的特征聚合配置
     */
    private static Map<String, List<IFunction>> FUNCTIONS_MAP_1H = new HashMap<>();

    /**
     * atom（原子）算子
     */
    private static Map<String, List<AtomFunction>> FUNCTIONS_MAP_ATOM = new HashMap<>();

    private static Map<String, IFunction> FEATURE_NAME_PREFIX_FUNCTION_MAP_1MIN = new HashMap<>();

    private static Map<String, String> FEATURE_BIZTYPE_MAP = new HashMap<>();

//    //redis
//    public static JedisPool flinkJedisPool = null;
//    public static JedisPool profileJedisPool = null;
//    //hbase
//    public static AliHBaseRepo aliHBaseRepo = null;
//    public static KafkaProducer<String, String> bigdatakafkaProducer;

    /**
     * 资源初始化
     *
     * @param configuration
     * @throws Exception
     */
    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
//        flinkJedisPool = ClientFactory.getFlinkJedisClient();
//        profileJedisPool = ClientFactory.getProfileJedisClient();
//        aliHBaseRepo = ClientFactory.getAliHBaseRepo();
//        bigdatakafkaProducer = ClientFactory.getBigDataKafkaProducer();

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("ScheduledGetConfig-%d")
                .setDaemon(true)
                .build();

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, threadFactory);
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                syncConfig();
            }
        }, 1, 10, TimeUnit.SECONDS);
    }

    private static void syncConfig() {
        if (localDateTime.equals(getUpdateTime())) {
            return;
        }
        localDateTime = getUpdateTime();
        //get config
        final List<FlinkConfigDto> configs = getConfig();
        log.info("get configs:{}", configs);

        //持久化配置转换
        SinkFactory.setHbaseSinkMap(configs);
        //特征聚合配置转换
        convert2FunctionsMap(configs);
        //获取特征-bizType映射关系
        convert2FeatureBizTypeMap(configs);
    }

    /**
     * 资源释放
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
//        if (Objects.nonNull(flinkJedisPool)) {
//            flinkJedisPool.close();
//        }
//        if (Objects.nonNull(profileJedisPool)) {
//            profileJedisPool.close();
//        }
//        if (Objects.nonNull(aliHBaseRepo)) {
//            aliHBaseRepo.closeCon();
//        }
//        if (Objects.nonNull(bigdatakafkaProducer)) {
//            bigdatakafkaProducer.close();
//        }
    }

    /**
     * do nothing
     *
     * @param value
     * @return
     * @throws Exception
     */
    @Override
    public Map<String, Object> map(Map<String, Object> value) throws Exception {
        return value;
    }

    private static LocalDateTime getUpdateTime() {
        RestTemplate restTemplate = new RestTemplate();
        final String updateUrl = ApolloConfigService.getapplicationConfig().getUpdateUrl();
        final ResponseEntity<String> forEntity = restTemplate.getForEntity(updateUrl + "/api/flinkConfig/getUpdateTime", String.class);
        final Response<String> response = JSON.parseObject(forEntity.getBody(), Response.class);
        final LocalDateTime localDateTime = LocalDateTime.parse(response.getResult());
        return localDateTime;
    }

    /**
     * 获取配置信息
     *
     * @return
     */
    private static List<FlinkConfigDto> getConfig() {
        RestTemplate restTemplate = new RestTemplate();
        final String updateUrl = ApolloConfigService.getapplicationConfig().getUpdateUrl();
        final ResponseEntity<String> forEntity = restTemplate.getForEntity(updateUrl + "/api/flinkConfig/get", String.class);
        final Response<List<JSONObject>> response = JSON.parseObject(forEntity.getBody(), Response.class);
        final List<JSONObject> results = response.getResult();
        final List<FlinkConfigDto> configs = results.stream().map(result -> {
            final FlinkConfigDto config = result.toJavaObject(FlinkConfigDto.class);
            return config;
        }).collect(Collectors.toList());
        return configs;
    }

    private static void convert2FunctionsMap(List<FlinkConfigDto> configs) {
        Map<String, List<IFunction>> tmpFunctionsMap_1s = new HashMap<>();
        Map<String, List<IFunction>> tmpFunctionsMap_1min = new HashMap<>();
        Map<String, List<IFunction>> tmpFunctionsMap_1h = new HashMap<>();
        Map<String, List<AtomFunction>> tmpFunctionsMap_atom = new HashMap<>();
        Map<String, IFunction> tmpFeatureNamePrefixFunctionMap_1min = new HashMap<>();

        for (FlinkConfigDto config : configs) {
            try {
                final IFunction function = FunctionFactory.getFunction(config);
                if (Objects.isNull(function)) {
                    continue;
                }

                final List<String> typeList = config.getTypeConfig().stream()
                        .map(item -> {
                            final String bizType = ((JSONObject) item).getString(BIZ_TYPE);
                            final String eventType = ((JSONObject) item).getString(EVENT_TYPE);
                            final String targetType = ((JSONObject) item).getString(TARGET_TYPE);
                            return bizType + eventType + targetType + config.getIdType();
                        }).collect(Collectors.toList());

                //如果是atom算子
                if ("atom".equals(function.getType())) {
                    typeList.forEach(type -> {
                        final List<AtomFunction> functions_atom = tmpFunctionsMap_atom.getOrDefault(type, new LinkedList<>());
                        functions_atom.add((AtomFunction) function);
                        tmpFunctionsMap_atom.put(type, functions_atom);
                    });
                    continue;
                }

                final Set<String> delaySet = config.getWindows().stream().map(window -> (JSONObject) window).map(window -> window.getString(DELAY)).collect(Collectors.toSet());
                delaySet.forEach(delay -> {
                    typeList.forEach(configType -> {
                        switch (delay) {
                            case "1s":
                                final List<IFunction> functions_1s = tmpFunctionsMap_1s.getOrDefault(configType, new LinkedList<>());
                                functions_1s.add(function);
                                tmpFunctionsMap_1s.put(configType, functions_1s);
                                break;
                            case "1min":
                            case "1m":
                                final List<IFunction> functions_1min = tmpFunctionsMap_1min.getOrDefault(configType, new LinkedList<>());
                                functions_1min.add(function);
                                tmpFunctionsMap_1min.put(configType, functions_1min);
                                tmpFeatureNamePrefixFunctionMap_1min.put(config.getFeatureNamePrefix(), function);
                                break;
                            case "1h":
                                final List<IFunction> functions_1h = tmpFunctionsMap_1h.getOrDefault(configType, new LinkedList<>());
                                functions_1h.add(function);
                                tmpFunctionsMap_1h.put(configType, functions_1h);
                                break;
                            default:
                                break;
                        }
                    });
                });
            } catch (Throwable throwable) {
                final String format = String.format("convert config error, error:%s", throwable);
                Cat.logError(format, throwable);
                log.error(format, throwable);
            }
        }

        FUNCTIONS_MAP_1S = tmpFunctionsMap_1s;
        FUNCTIONS_MAP_1MIN = tmpFunctionsMap_1min;
        FUNCTIONS_MAP_1H = tmpFunctionsMap_1h;
        FUNCTIONS_MAP_ATOM = tmpFunctionsMap_atom;
        FEATURE_NAME_PREFIX_FUNCTION_MAP_1MIN = tmpFeatureNamePrefixFunctionMap_1min;
        log.info("config update, FUNCTIONS_MAP_1S:{}", FUNCTIONS_MAP_1S);
        log.info("config update, FUNCTIONS_MAP_1MIN:{}", FUNCTIONS_MAP_1MIN);
        log.info("config update, FUNCTIONS_MAP_1H:{}", FUNCTIONS_MAP_1H);
        log.info("config update, FUNCTIONS_MAP_ATOM:{}", FUNCTIONS_MAP_ATOM);
        log.info("config update, FEATURE_NAME_PREFIX_FUNCTION_MAP_1MIN:{}", FEATURE_NAME_PREFIX_FUNCTION_MAP_1MIN);
    }

    public static Map<String, List<IFunction>> get1sFunctionsMap() {
        if (MapUtils.isEmpty(FUNCTIONS_MAP_1S)) {
            syncConfig();
        }
        return FUNCTIONS_MAP_1S;
    }

    public static Map<String, List<IFunction>> get1mFunctionsMap() {
        if (MapUtils.isEmpty(FUNCTIONS_MAP_1MIN)) {
            syncConfig();
        }
        return FUNCTIONS_MAP_1MIN;
    }

    public static Map<String, List<IFunction>> get1hFunctionsMap() {
        if (MapUtils.isEmpty(FUNCTIONS_MAP_1H)) {
            syncConfig();
        }
        return FUNCTIONS_MAP_1H;
    }

    public static Map<String, List<AtomFunction>> getAtomFunctionsMap() {
        if (MapUtils.isEmpty(FUNCTIONS_MAP_ATOM)) {
            syncConfig();
        }
        return FUNCTIONS_MAP_ATOM;
    }

    public static Map<String, IFunction> get1mFeatureNamePrefixFunctionMap() {
        if (MapUtils.isEmpty(FEATURE_NAME_PREFIX_FUNCTION_MAP_1MIN)) {
            syncConfig();
        }
        return FEATURE_NAME_PREFIX_FUNCTION_MAP_1MIN;
    }

    public static Map<String, String> getFeatureBiztypeMap() {
        if (MapUtils.isEmpty(FEATURE_BIZTYPE_MAP)) {
            syncConfig();
        }
        return FEATURE_BIZTYPE_MAP;
    }

    private static void convert2FeatureBizTypeMap(List<FlinkConfigDto> configs) {
        Map<String, String> tmpFeatureBizTypeMap = new HashMap<>();
        configs.forEach(config -> {
            config.getSinks().toJavaList(JSONObject.class).forEach(sinkConfig -> {
                final String type = sinkConfig.getString("type");
                if ("hbase".equals(type.toLowerCase())) {
                    final JSONObject fields = sinkConfig.getJSONObject("fields");
                    Map<String, JSONObject> fieldsJson = JSONObject.parseObject(fields.toJSONString(), new TypeReference<Map<String, JSONObject>>() {
                    });
                    fieldsJson.forEach((k, v) -> {
                        String featureName = (v.containsKey("rename") && StringUtils.isNotBlank(v.getString("rename"))) ? v.getString("rename") : k;
                        tmpFeatureBizTypeMap.put(featureName, config.getBizType());
                    });
                }
            });
        });
        FEATURE_BIZTYPE_MAP = tmpFeatureBizTypeMap;
    }
}
