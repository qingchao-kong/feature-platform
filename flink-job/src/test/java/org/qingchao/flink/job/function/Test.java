package org.qingchao.flink.job.function;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import static org.qingchao.flink.job.function.AbstractFunction.MAP_TYPE_REFERENCE;
import static org.qingchao.flink.job.function.AbstractFunction.TUPLE_LIST_TYPE_REFERENCE;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-01-09 6:02 下午
 */
@Slf4j
public class Test {

    @org.junit.Test
    public void test(){
        final LinkedHashMap<String,Long> map = Maps.newLinkedHashMap();
        {
            map.put("1",1L);
            map.put("2",2L);
            map.put("3",3L);
            map.put("4",4L);
        }
        final String jsonString = JSON.toJSONString(map);
        final Object o1 = JSON.parseObject(jsonString, MAP_TYPE_REFERENCE);

        List<Tuple2<String,Long>> list=new LinkedList<>();
        {
            list.add(Tuple2.of("1",1L));
            list.add(Tuple2.of("2",2L));
        }
        final String listJsonString = JSON.toJSONString(list);
//        final List<Object> objects = JSON.parseArray(listJsonString, TUPLE_TYPE_ARRAY);
        final Object o = JSON.parseObject(listJsonString, TUPLE_LIST_TYPE_REFERENCE);
//        log.info(objects.toString());
        log.info("ok");
    }

    @org.junit.Test
    public void wholeWindow_Test(){
        Long currentTimestamps=System.currentTimeMillis();
        Long oneDayTimestamps= Long.valueOf(60*60*24*1000);
        final long l = currentTimestamps - (currentTimestamps + 60 * 60 * 8 * 1000) % oneDayTimestamps;
        log.info("l:{}",l);

        Long  time = System.currentTimeMillis();  //当前时间的时间戳
        long zero = time/(1000*3600*24)*(1000*3600*24) - TimeZone.getDefault().getRawOffset();
        log.info("zero:{}",zero);
    }
}
