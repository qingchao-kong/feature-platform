package org.qingchao.flink.job.repo.kv;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RawKeyValue {
    /**
     * 列名
     */
    private byte[] key;
    /**
     * 时间戳 单位(ms)
     */
    private long ts;
    /**
     * 列值
     */
    private byte[] value;

    public int getByteSize(){
        return key.length + value.length;
    }
}
