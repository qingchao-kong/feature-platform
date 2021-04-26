package org.qingchao.flink.job.base;

/**
 * @author kongqingchao
 * @date 2020/12/3
 */

import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

public interface ServiceBase {
    /**
     * @return 类型
     */
    String getTransactionType();

    /**
     * @param transName     名称
     * @param invoke        方法
     * @param exceptionThen 异常
     * @param <T>           泛型
     * @return 结果
     */
    default <T> T inTransaction(String transName, SupplierWithException<T> invoke, T exceptionThen) {
        Transaction transaction = Cat.newTransaction(getTransactionType(), transName);
        T result;
        try {
            result = invoke.get();
            transaction.setSuccessStatus();
        } catch (Exception e) {
            transaction.setStatus(e);
            Cat.logError(String.format("t:%s-%s", getTransactionType(), transName), e);
            result = exceptionThen;
        } finally {
            transaction.complete();
        }
        return result;
    }

    /**
     * @param transName     名称
     * @param invoke        方法
     * @param exceptionThen 异常
     * @param <T>           泛型
     * @return 结果
     */
    static <T> T withTransaction(String transType, String transName, SupplierWithException<T> invoke, T exceptionThen) {
        Transaction transaction = Cat.newTransaction(transType, transName);
        T result;
        try {
            result = invoke.get();
            transaction.setSuccessStatus();
        } catch (Exception e) {
            transaction.setStatus(e);
            Cat.logError(String.format("t:%s-%s", transType, transName), e);
            result = exceptionThen;
        } finally {
            transaction.complete();
        }
        return result;
    }
}

