package org.qingchao.flink.job.repo.kv;

import lombok.Data;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Objects;

@Data
public class RawRow {
    String rowKey;
    /**
     * 用来标识columns是否有值
     */
    private boolean empty = false;
    private List<RawKeyValue> columns;
    private long ttl;

    public int getByteSize() {
        int length = Bytes.toBytes(rowKey).length;
        return length + (empty ? 0 : columns.stream().mapToInt(RawKeyValue::getByteSize).sum());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RawRow row = (RawRow) o;
        return Objects.equals(rowKey, row.getRowKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKey);
    }

}
