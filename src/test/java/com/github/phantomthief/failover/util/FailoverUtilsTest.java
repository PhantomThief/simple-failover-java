/**
 * 
 */
package com.github.phantomthief.failover.util;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.commons.lang3.RandomUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.github.phantomthief.failover.Failover;
import com.github.phantomthief.failover.impl.WeightFailover;
import com.google.common.collect.ImmutableMap;

/**
 * @author w.vela
 */
public class FailoverUtilsTest {

    interface Call {

        String call(int num);
    }

    class NormalCall implements Call {

        @Override
        public String call(int num) {
            return String.valueOf(num);
        }
    }

    class FailCall implements Call {

        @Override
        public String call(int num) {
            throw new RuntimeException(String.valueOf(num));
        }
    }

    @Ignore
    @Test
    public void testProxy() throws Exception {
        Failover<Call> failover = WeightFailover.<Call> newGenericBuilder() //
                .checkDuration(1, SECONDS) //
                .checker(call -> true) //
                .build(ImmutableMap.of(new NormalCall(), 10, new FailCall(), 10));
        Call proxy = FailoverUtils.proxy(Call.class, failover);
        for (int i = 0; i < 10; i++) {
            try {
                System.out.println(proxy.call(RandomUtils.nextInt(0, 100)));
            } catch (Throwable e) {
                e.printStackTrace();
            }
            sleepUninterruptibly(1, SECONDS);
        }
    }
}
