package com.github.phantomthief.failover;

/**
 * @author w.vela
 * Created on 2019-01-04.
 */
public class WeighTestUtils {

    private static final double OFFSET = 0.3;

    public static boolean checkRatio(int a, int b, int ratio) {
        return between((double) a / b, (double) ratio - OFFSET, (double) ratio + OFFSET);
    }

    private static boolean between(double k, double min, double max) {
        return min <= k && k <= max;
    }
}
