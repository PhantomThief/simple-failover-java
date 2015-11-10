/**
 * 
 */
package com.github.phantomthief.failover.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * @author w.vela
 */
public class RandomListUtils {

    private static final Random RANDOM = new Random();

    private RandomListUtils() {
        throw new UnsupportedOperationException();
    }

    public static <T> T getRandom(List<T> source) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        return source.get(RANDOM.nextInt(source.size()));
    }

    public static <T> List<T> getRandom(List<T> source, int size) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> newList = new ArrayList<>(source);
        Collections.shuffle(newList, RANDOM);
        return newList.subList(0, Math.min(newList.size(), size));
    }
}
