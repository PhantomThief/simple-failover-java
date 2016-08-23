/**
 * 
 */
package com.github.phantomthief.failover.util;

import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;

import java.util.ArrayList;
import java.util.Collection;
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

    /**
     * use {@link #getRandom(Collection, int)}
     */
    @Deprecated
    public static <T> List<T> getRandom(List<T> source, int size) {
        return getRandom((Collection<T>) source, size);
    }

    public static <T> List<T> getRandom(Collection<T> source, int size) {
        if (source == null || source.isEmpty()) {
            return emptyList();
        }
        List<T> newList = new ArrayList<>(source);
        shuffle(newList, RANDOM);
        return newList.subList(0, min(newList.size(), size));
    }
}
