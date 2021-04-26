package org.qingchao.flink.job.function;

import java.util.Map;

/**
 * 描述:自定义算子接口
 *
 * @author kongqingchao
 * @create 2020-12-01 10:37 上午
 */
public interface IFunction {
    /*-----------------以下为1min窗口 agg逻辑-------------------------*/

//    /**
//     * 每个window每个id第一条数据输入时的处理逻辑
//     */
//    void createAccumulator_1min(Map<String, Object> accumulator);

    /**
     * 每个window每个id第二条数据及后面数据的处理逻辑
     *
     * @param value
     * @param accumulator
     * @return
     */
    void add_1min(Map<String, Object> value, Map<String, Object> accumulator);

    /**
     * 各subTask内部处理后，reduce逻辑
     *
     * @param aAcc
     * @param bAcc
     * @return
     */
    void merge_1min(Map<String, Object> aAcc, Map<String, Object> bAcc);

//    /**
//     * 得到最终结果逻辑
//     *
//     * @param accumulator
//     * @return
//     */
//    Map<String, Object> getResult_1min(Map<String, Object> accumulator);

    /*----------------------以下为1min窗口聚合后，生成最终特征逻辑------------------------------*/

    /**
     * 获取所有中间结果，保存此次window得到的中间结果，计算最终特征
     *
     * @param value
     */
    void kv_1min(Map<String, Object> value);

    String getType();
}
