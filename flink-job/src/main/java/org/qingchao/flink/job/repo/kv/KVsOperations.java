package org.qingchao.flink.job.repo.kv;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface KVsOperations {

    /**
     * 根据表名及rowKey明确的返回确定的row,依据columnPrefix进行过滤
     *
     * @return 获取不到对应的rowKey 返回 {@code Optional.empty()}
     */
    Optional<RawRow> get(String rowKey, String tablename);

    /**
     * 根据表名及rowKeys明确的返回确定的rows,依据columnPrefix进行过滤
     *
     * @return 获取不到返回 {@code null}
     */
    List<RawRow> get(Collection<String> rowKeys, String tablename, String columnPrefix);

    boolean put(RawRow row, String tablename);

    boolean put(Collection<RawRow> row, String tablename);

    boolean delete(String rowKey, String tablename, String columnPrefix);
}
