package org.qingchao.flink.job.base;

/**
 * @author kongqingchao
 * @date 2020/12/3
 */
@FunctionalInterface
public interface SupplierWithException<T> {
    T get() throws Exception;
}

