package org.qingchao.flink.job.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.spring.annotation.ApolloConfig;
import lombok.Data;

/**
 * @author kongqingchao
 * @version 1.0
 * @date 2020/12/8 11:17 上午
 */
@Data
public class HBaseConfig {

    @ApolloConfig("middleware.hbase.aitm-profile-service")
    private Config adminConfig;

    @ApolloConfig()
    private Config appConfig;

    private String endpoint;

    private String username;

    private String password;

    private Integer putRate;

    private Integer getRate;

    private Integer poolSize;

    private String namespace;



    public void init(){
        endpoint = adminConfig.getProperty("hbase.client.endpoint",
                "ld-bp1lxahf014ub02s1-proxy-hbaseue.hbaseue.rds.aliyuncs.com:30020");
        username = adminConfig.getProperty("hbase.client.username",
                "test");
        password = adminConfig.getProperty("hbase.client.password",
                "test");
        putRate=appConfig.getIntProperty("AliHBase.rate.put",10);
        getRate=appConfig.getIntProperty("AliHBase.rate.get",50);
        poolSize=appConfig.getIntProperty("AliHBase.poolSize",10);
        namespace=appConfig.getProperty("AliHBase.namespace","aitm_profile");

    }






}
