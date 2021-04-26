package org.qingchao.flink.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.qingchao.flink.job.streamfunction.*;
import org.qingchao.flink.job.streamfunction.atom.AtomFilterFunction;
import org.qingchao.flink.job.streamfunction.atom.AtomProcessFunction;
import org.qingchao.flink.job.streamfunction.tublingwindow.AggregateOneMinFunction;
import org.qingchao.flink.job.streamfunction.tublingwindow.MiddleFeatureOneMinFilterFunction;
import org.qingchao.flink.job.streamfunction.tublingwindow.OneMinTumblingWindowFilterFunction;
import org.qingchao.flink.job.streamfunction.tublingwindow.ProcessMiddleFeature1MinFunction;

import java.util.Map;
import java.util.Objects;

import static org.qingchao.flink.job.constant.Constant._ID_TYPE;


/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-01 7:30 下午
 */
@Slf4j
public class JobBase {
    public static void main(StreamExecutionEnvironment env, DataStream<Map<String, Object>> allStream) throws Exception {
        //5。数据预处理
        final SingleOutputStreamOperator<Map<String, Object>> preHandleStream = allStream
                //init resource
                .map(new ResourceInitRichMapFunction())
                .filter(Objects::nonNull)
                //pre handle
                .map(new PreHandleMapFunction())
                .filter(Objects::nonNull);

        //6.特征处理
        //6.1.atom（原子）特征处理
        final SingleOutputStreamOperator<Map<String, Object>> atomFeatureStream = preHandleStream
                .filter(new AtomFilterFunction())
                .process(new AtomProcessFunction());

        //6.2.滚动窗口处理1min特征
        final SingleOutputStreamOperator<Map<String, Object>> tublingWindowFeatureStream = preHandleStream
                //判断数据是否需要进行滚动窗口聚合
                .filter(new OneMinTumblingWindowFilterFunction())
                //key by _id
                .keyBy(new KeySelectorFunction())
                .timeWindow(Time.minutes(1))
                .aggregate(new AggregateOneMinFunction())
                //过滤空的中间结果
                .filter(new MiddleFeatureOneMinFilterFunction())
                .process(new ProcessMiddleFeature1MinFunction());

        //6.3.滑动窗口处理1s特征，5分钟窗口，1s滑动
//        final SingleOutputStreamOperator<Map<String, Object>> slidingWindowFeatureStream = preHandleStream
//                //过滤不需要进行1s级聚合的数据
//                .filter(new SlidingWindowFilterFunction())
//                //key by _id
//                .keyBy(new KeySelectorFunction())
//                //滑动窗口1s，聚合窗口5min
//                .timeWindow(Time.minutes(5), Time.seconds(1))
//                //聚合处理
//                .aggregate(new AggregateSlidingWindowFunction());

        //7.连接所有特征结果
        final DataStream<Map<String, Object>> allFeatureStream = atomFeatureStream.union(tublingWindowFeatureStream);

        //8.持久化
        allFeatureStream
                .filter(new KvFilterFunction())
                .keyBy(new KeySelectorFunction())
                .timeWindow(Time.seconds(1))
                //自定义窗口
                .trigger(new CountTriggerWithTimeout<>(100, TimeCharacteristic.ProcessingTime))
                .process(new BatchSinkFunction());

        //todo 数据向下游发送，进行多级窗口聚合

        env.execute();
    }

    public static SingleOutputStreamOperator<Map<String, Object>> addIdType(SingleOutputStreamOperator<Map<String, Object>> deserialStream, String idType) {
        return deserialStream.map(new MapFunction<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Map<String, Object> value) throws Exception {
                value.put(_ID_TYPE, idType);
                return value;
            }
        });
    }

}
