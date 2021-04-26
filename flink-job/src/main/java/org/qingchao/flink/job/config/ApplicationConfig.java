package org.qingchao.flink.job.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/4 4:41 下午
 */
@Data
@Slf4j
public class ApplicationConfig implements Serializable {
    @ApolloConfig("application")
    private Config appConfig;

    private String updateUrl;

    public void init() {
        try {
            updateUrl = appConfig.getProperty("config.update.url","https://test-manage.xxx.com");
        }catch (Throwable throwable){
            log.error("convert keyTypeSet error, error:{}",throwable,throwable);
        }
    }
}
