package org.qingchao.flink.job.aviator;

import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * 描述:
 *
 * @author kongqingchao
 * @create 2020-12-11 2:50 下午
 */
@Slf4j
public class AviatorTest {

    @Test
    public void aviator_Test(){
        Map<String, Object> map = new HashMap<String,Object>(){{
            put("uid","123");
            Map<String, Object> ext = new HashMap<String, Object>(){{
                put("pay",33);
            }};
            put("ext",ext);
        }};

        final Object execute = AviatorEvaluator.execute("ext.pay", map);
        log.info(execute.toString());
    }
}
