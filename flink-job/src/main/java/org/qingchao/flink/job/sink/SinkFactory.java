package org.qingchao.flink.job.sink;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.dianping.cat.Cat;
import lombok.extern.slf4j.Slf4j;
import org.qingchao.flink.job.config.FlinkConfigDto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.qingchao.flink.job.base.ServiceBase.withTransaction;

/**
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/19 3:54 下午
 */
@Slf4j
public class SinkFactory {
    private static Map<String, JSONObject> hbaseSinkMap = new HashMap<>();
    private static Map<String, JSONObject> kafkaSinkMap = new HashMap<>();

    public static void setHbaseSinkMap(List<FlinkConfigDto> flinkConfigDtoList) {
        Map<String, JSONObject> tmpHbaseSinkMap = new HashMap<>();
        flinkConfigDtoList.forEach(flinkConfigDto -> {
            try {
                flinkConfigDto.getSinks().toJavaList(JSONObject.class).forEach(sinkConfig -> {
                    final String type = sinkConfig.getString("type");
                    final JSONObject fields = sinkConfig.getJSONObject("fields");
                    withTransaction("SinkFactory", String.format("%s", type),
                            () -> {
                                switch (type.toLowerCase()) {
                                    case "hbase":
                                        Map<String, JSONObject> fieldsJson = JSONObject.parseObject(fields.toJSONString(), new TypeReference<Map<String, JSONObject>>() {
                                        });
                                        tmpHbaseSinkMap.putAll(fieldsJson);
                                        break;
                                    case "kafka":
                                        break;
                                    default:
                                        break;
                                }
                                return true;
                            },
                            null);
                });
            } catch (Throwable throwable) {
                final String format = String.format("convert config to sinkMap error, config:%s, error:%s", flinkConfigDto, throwable);
                log.error(format, throwable);
                Cat.logError(format, throwable);
            }
        });
        hbaseSinkMap = tmpHbaseSinkMap;
    }

    public static Map<String, JSONObject> createHbaseSink() {
        return hbaseSinkMap;
    }

    public static Map<String, JSONObject> createKafkaSink() {
        return kafkaSinkMap;
    }
}
