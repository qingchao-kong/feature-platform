package org.qingchao.flink.job.function;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2021-02-19 3:34 下午
 */
@Slf4j
public class ListFunctionTest {

    @Test
    public void test(){
        final LinkedList<String> list = new LinkedList<String>() {{
            add("0");
            add("0");
            add("0");
            add("0");
            add("0");
            add("0");
        }};
        final List<String> strings = list.subList(0, 1);
        final int min = Math.min(list.size(), 100);
        final List<String> strings1 = list.subList(0, min);
        log.info("ok");
    }
}
