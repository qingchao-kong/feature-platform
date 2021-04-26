package org.qingchao.flink.job.config;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-07 11:36 上午
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FlinkConfigDto implements Serializable {
    private Integer id;

    /**
     * 算子类型
     */
    private String configType;

    private JSONArray typeConfig;

    /**
     * 业务配置
     */
    private String bizType;

    /**
     * 业务配置
     */
    private String targetType;

    /**
     * 业务配置
     */
    private String eventType;

    /**
     * 统计维度字段
     */
    private String idField;

    /**
     * 维度类型
     */
    private String idType;

    /**
     * 特征名字前缀
     */
    private String featureNamePrefix;

    /**
     * 聚合配置
     */
    private JSONObject aggConfig;

    private String delay;

    private JSONArray windows;

    /**
     * 落库
     */
    private JSONArray sinks;

    /**
     * 逻辑删除标志位
     */
    private Byte status;

    /**
     * 描述
     */
    private String description;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    private LocalDateTime updateTime;
}
