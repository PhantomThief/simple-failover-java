package com.github.phantomthief.failover.util;

import static com.github.phantomthief.failover.util.RandomListUtils.getRandomUsingLcg;
import static com.github.phantomthief.failover.util.RandomListUtils.getRandomUsingShuffle;
import static java.lang.System.nanoTime;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author w.vela
 * Created on 2018-02-26.
 */
class RandomListUtilsTest {

    private static final Logger logger = LoggerFactory.getLogger(RandomListUtilsTest.class);

    @Test
    void getRandom() {
        long lcgBetter = 0, shuffleBetter = 0;
        for (int i = 0; i < 1000; i++) {
            int size = ThreadLocalRandom.current().nextInt(1, 10000);
            int retrieved = ThreadLocalRandom.current().nextInt(1, Math.max(2, size / 3));
            List<Integer> list = IntStream.range(0, size).boxed().collect(toList());

            long s = nanoTime();
            List<Integer> result = getRandomUsingLcg(list, retrieved);
            long costLcg = nanoTime() - s;

            long s2 = nanoTime();
            getRandomUsingShuffle(list, retrieved);
            long costShuffle = nanoTime() - s2;

            assertEquals(retrieved, result.stream().distinct().count());
            if (costLcg > costShuffle) {
                shuffleBetter += costLcg - costShuffle;
            } else {
                lcgBetter += costShuffle - costLcg;
            }
        }
        logger.info("lcg better:{}, shuffle better:{}", lcgBetter, shuffleBetter);
        lcgBetter = 0;
        shuffleBetter = 0;
        for (int i = 0; i < 1000; i++) {
            int size = ThreadLocalRandom.current().nextInt(1, 10000);
            List<Integer> list = IntStream.range(0, size).boxed().collect(toList());

            long s = nanoTime();
            List<Integer> result = getRandomUsingLcg(list, size);
            long costLcg = nanoTime() - s;

            long s2 = nanoTime();
            getRandomUsingShuffle(list, size);
            long costShuffle = nanoTime() - s2;

            assertEquals(size, result.stream().distinct().count());
            if (costLcg > costShuffle) {
                shuffleBetter += costLcg - costShuffle;
            } else {
                lcgBetter += costShuffle - costLcg;
            }
        }
        logger.info("lcg better:{}, shuffle better:{}", lcgBetter, shuffleBetter);
    }
}