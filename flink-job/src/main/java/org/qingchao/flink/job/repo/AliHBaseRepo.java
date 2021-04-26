package org.qingchao.flink.job.repo;

import com.dianping.cat.Cat;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.qingchao.flink.job.base.ServiceBase;
import org.qingchao.flink.job.repo.kv.KVsOperations;
import org.qingchao.flink.job.repo.kv.RawRow;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * HBase数据库基本操作
 *
 * @author kongqingchao
 */
@Slf4j
public class AliHBaseRepo implements ServiceBase, KVsOperations {


    private static String FAMILY_COLUMN = "0";

    private Connection connection = null;
    // 创建一个定长线程池，可控制线程最大并发数，超出的线程会在队列中等待。
    private static ExecutorService executor = null;
    private String namespace;
    private RateLimiter getRate;
    private RateLimiter putRate;

    private Double putPerSecond;
    private Double getPerSecond;

    public Double getGetPerSecond() {
        return getPerSecond;
    }

    public Double getPutPerSecond() {
        return putPerSecond;
    }

    public AliHBaseRepo(Configuration conf, int poolSize, String namespace, double getPerSecond, double putPerSecond) {
        try {
            executor = new ThreadPoolExecutor(
                    5, // 核心工作线程： 单机 标配 4C8G 5G JVM估算， 30个活动线程
                    20, // 最大 线程数
                    10L, TimeUnit.SECONDS, //线程空闲等待时间，
                    new LinkedBlockingQueue<Runnable>(20),//超过最大阻塞等待队列，会申请核心工作线程直到 max
                    new ThreadPoolExecutor.CallerRunsPolicy() //线程池无法响应时，由调用者执行
            );

            // 创建 HBase连接，在程序生命周期内只需创建一次，该连接线程安全，可以共享给所有线程使用。
            // 在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。
            // 也可以采用try finally方式防止泄露
            connection = ConnectionFactory.createConnection(conf, executor);
            this.namespace = namespace;
            this.getPerSecond = getPerSecond;
            this.putPerSecond = putPerSecond;
            this.getRate = RateLimiter.create(getPerSecond);
            this.putRate = RateLimiter.create(putPerSecond);
        } catch (IOException e) {
            log.error("获取HBase连接失败");
        }
    }


    @Override
    public Optional<RawRow> get(String rowKey, String tablename) {
        return Optional.empty();
    }

    @Override
    public List<RawRow> get(Collection<String> rowKeys, String tablename, String columnPrefix) {
        return null;
    }

    @Override
    public boolean put(RawRow row, String tablename) {
        try {
            final Put put = transformRowToPut(row);
            // 获取表
            Table table = connection.getTable(TableName.valueOf(namespace, tablename));
            table.put(put);
            table.close();
            //cache.delete(rowKey, tablename, columnPrefix);
            return true;
        } catch (Exception e) {
            Cat.logError(e);
        }
        return false;
    }

    @Override
    public boolean put(Collection<RawRow> row, String tablename) {
        try{
            final List<Put> puts = row.stream().map(rawRow -> {
                return transformRowToPut(rawRow);
            }).collect(Collectors.toList());
            // 获取表
            Table table = connection.getTable(TableName.valueOf(namespace, tablename));
            table.put(puts);
            table.close();
        }catch (Exception e){
            Cat.logError(e);
        }
        return true;
    }

    @Override
    public boolean delete(String rowKey, String tablename, String columnPrefix) {
        try {
            // 获取表
            Table table = connection.getTable(TableName.valueOf(namespace, tablename));

            Optional<RawRow> columns = get(rowKey, tablename);
            if (!columns.isPresent()) {
                return false;
            }
            RawRow rawRow = columns.get();
            if (rawRow.isEmpty()) {
                return true;
            }
            // 设置rowKey
            byte[] rawRowKey = Bytes.toBytes(rowKey);
            byte[] rawFamily = Bytes.toBytes(FAMILY_COLUMN);
            Delete op = new Delete(rawRowKey);

            rawRow.getColumns().forEach(raw -> {
                op.addColumn(rawFamily, raw.getKey());
            });

            table.delete(op);
            table.close();

            //cache.delete(rowKey, tablename, columnPrefix);
            return true;
        } catch (Exception e) {
            Cat.logError(e);
        }
        return false;
    }

    public void closeCon() throws IOException {
        this.connection.close();
    }

    @Override
    public String getTransactionType() {
        return "AliHBase";
    }

    private Put transformRowToPut(RawRow row){
        if (!putRate.tryAcquire(100, TimeUnit.MILLISECONDS)) {
            //throw new ExceedRateLimitException("exceed put rate limit");
            throw new RuntimeException("exceed put rate limit");
        }
        // 设置rowkey
        String rowKey = row.getRowKey();
        byte[] rawRowkey = Bytes.toBytes(rowKey);
        byte[] rawFamily = Bytes.toBytes(FAMILY_COLUMN);
        Put put = new Put(rawRowkey);

        row.getColumns().forEach(raw -> {
            put.addColumn(rawFamily, raw.getKey(), raw.getTs(), raw.getValue());
        });
        return put;
    }
}