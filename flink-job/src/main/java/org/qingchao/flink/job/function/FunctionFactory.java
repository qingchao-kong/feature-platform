package org.qingchao.flink.job.function;

import org.qingchao.flink.job.config.FlinkConfigDto;

import static org.qingchao.flink.job.base.ServiceBase.withTransaction;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-02 2:22 下午
 */
public class FunctionFactory {

    public static IFunction getFunction(FlinkConfigDto config) {
        return withTransaction("FunctionFactory", String.format("%s-%s-%s-%s", config.getBizType(), config.getConfigType(), config.getIdType(), config.getDelay()),
                () -> {
                    switch (config.getConfigType().toLowerCase()) {
                        case "count":
                            return new CountFunction(config);
                        case "sum":
                            return new SumFunction(config);
                        case "max":
                            return new MaxFunction(config);
                        case "min":
                            return new MinFunction(config);
                        case "list":
                            return new ListFunction(config);
                        case "set":
                            return new SetFunction(config);
                        case "latest":
                            return new LatestFunction(config);
                        case "atom":
                            return new AtomFunction(config);
                        default:
                            return null;
                    }
                },
                null);
    }
}
