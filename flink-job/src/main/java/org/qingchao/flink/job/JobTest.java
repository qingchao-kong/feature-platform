package org.qingchao.flink.job;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-01-28 2:16 下午
 */
@Slf4j
public class JobTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<String> list=new ArrayList<String>(){{
            add("{\"id\":\"0\"}");
            add("{\"id\":\"1\"}");
            add("{\"id\":\"2\"}");
            add("{\"id\":\"3\"}");
            add("{\"id\":\"4\"}");
            add("{\"id\":\"5\"}");
        }};
        final DataStreamSource<String> stream = env.fromCollection(list);

        final SingleOutputStreamOperator<Map<String, Object>> deserializeStream = stream.map(new MapFunction<String, Map<String,Object>>() {
            @Override
            public Map<String, Object> map(String value) throws Exception {
                final Map map = JSON.parseObject(value, Map.class);
                final Map<String, Object> map1 = map;
                return map1;
            }
        });

        final SingleOutputStreamOperator<Map<String, Object>> map1 = deserializeStream.map(new MapFunction<Map<String, Object>, Map<String,Object>>() {
            @Override
            public Map<String, Object> map(Map<String, Object> value) throws Exception {
                value.put("idType1", "id1");
                return value;

            }
        });

        final SingleOutputStreamOperator<Map<String, Object>> map2 = deserializeStream.map(new MapFunction<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(Map<String, Object> value) throws Exception {
                value.put("idType2", "id2");
                return value;
            }
        });

        map1.print("map1:");
        map2.print("map2:");

        env.execute();

//print result:
//        map1:> {idType1=id1, id=0}
//        map2:> {idType2=id2, id=0}
//        map1:> {idType1=id1, id=1}
//        map2:> {idType2=id2, id=1}
//        map1:> {idType1=id1, id=2}
//        map2:> {idType2=id2, id=2}
//        map1:> {idType1=id1, id=3}
//        map2:> {idType2=id2, id=3}
//        map1:> {idType1=id1, id=4}
//        map2:> {idType2=id2, id=4}
//        map1:> {idType1=id1, id=5}
//        map2:> {idType2=id2, id=5}
    }
}
