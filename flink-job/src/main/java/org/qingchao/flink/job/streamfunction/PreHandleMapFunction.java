package org.qingchao.flink.job.streamfunction;

import com.dianping.cat.Cat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.rmi.server.UID;
import java.util.Map;

import static org.qingchao.flink.job.constant.Constant.*;

/**
 * 描述:数据预处理
 *
 * @author kongqingchao
 * @create 2020-12-08 3:18 下午
 */
@Slf4j
public class PreHandleMapFunction implements MapFunction<Map<String, Object>, Map<String, Object>> {
    @Override
    public Map<String, Object> map(Map<String, Object> value) {
        log.debug("enter PreHandleMapFunction.map, value:{}", value);
        try {
            String _idType = (String) value.get(_ID_TYPE);

            String _id = "";
            if (UID.equals(_idType)) {
                _id = value.getOrDefault(UID, "").toString();
            } else {
                Map<String, Object> ext = (Map) value.get(EXT);
                _id = ext.getOrDefault(_idType, "").toString();
            }

            //check blank
            if (StringUtils.isBlank(_id)) {
                return null;
            }
            value.put(_ID, _id);

            final String bizType = (String) value.getOrDefault(BIZ_TYPE, "");
            final String eventType = (String) value.getOrDefault(EVENT_TYPE, "");
            final String targetType = (String) value.getOrDefault(TARGET_TYPE, "");
            String _configType = bizType + eventType + targetType + _idType;
            value.put(_CONFIG_TYPE, _configType);

            log.debug("exit PreHandleMapFunction.map, value:{}", value);
            return value;
        } catch (Throwable throwable) {
            final String format = String.format("pre handle error, value:%s, error:%s", value, throwable);
            Cat.logError(format, throwable);
            log.error(format, throwable);
            return null;
        }
    }
}
