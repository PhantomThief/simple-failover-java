package com.github.phantomthief.failover.util;

import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

/**
 * @author w.vela
 */
public class RandomListUtils {

    private RandomListUtils() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    public static <T> T getRandom(List<T> source) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        return source.get(ThreadLocalRandom.current().nextInt(source.size()));
    }

    public static <T> List<T> getRandom(Collection<T> source, int size) {
        if (source == null || source.isEmpty()) {
            return emptyList();
        }
        List<T> newList = new ArrayList<>(source);
        shuffle(newList, ThreadLocalRandom.current());
        return newList.subList(0, min(newList.size(), size));
    }
}
