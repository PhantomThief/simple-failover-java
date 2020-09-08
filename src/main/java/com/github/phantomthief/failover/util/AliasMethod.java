package com.github.phantomthief.failover.util;

import static java.util.Objects.requireNonNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nonnull;

/**
 * http://www.keithschwarz.com/darts-dice-coins/
 *
 * @author w.vela
 * Created on 2020-04-07.
 */
public class AliasMethod<T> {

    private final Object[] values;
    private final int[] alias;
    private final double[] probability;

    public AliasMethod(@Nonnull Map<T, ? extends Number> weightMap) {
        requireNonNull(weightMap);
        if (weightMap.isEmpty()) {
            throw new IllegalArgumentException("weightMap is empty");
        }
        List<Double> probabilities = new ArrayList<>(weightMap.size());
        List<T> valueList = new ArrayList<>(weightMap.size());
        double sum = 0;
        for (Entry<T, ? extends Number> entry : weightMap.entrySet()) {
            double weight = entry.getValue().doubleValue();
            if (weight > 0) {
                sum += weight;
                valueList.add(entry.getKey());
            }
        }
        for (Entry<T, ? extends Number> entry : weightMap.entrySet()) {
            double weight = entry.getValue().doubleValue();
            if (weight > 0) {
                probabilities.add(weight / sum);
            }
        }
        if (sum <= 0) {
            throw new IllegalArgumentException("invalid weight map:" + weightMap);
        }
        values = valueList.toArray(new Object[0]);

        int size = probabilities.size();
        probability = new double[size];
        alias = new int[size];

        double average = 1.0 / size;

        Deque<Integer> small = new ArrayDeque<>();
        Deque<Integer> large = new ArrayDeque<>();

        for (int i = 0; i < size; ++i) {
            if (probabilities.get(i) >= average) {
                large.add(i);
            } else {
                small.add(i);
            }
        }

        while (!small.isEmpty() && !large.isEmpty()) {
            int less = small.removeLast();
            int more = large.removeLast();

            probability[less] = probabilities.get(less) * size;
            alias[less] = more;

            probabilities.set(more, probabilities.get(more) + probabilities.get(less) - average);

            if (probabilities.get(more) >= average) {
                large.add(more);
            } else {
                small.add(more);
            }
        }

        while (!small.isEmpty()) {
            probability[small.removeLast()] = 1.0;
        }
        while (!large.isEmpty()) {
            probability[large.removeLast()] = 1.0;
        }
    }

    @SuppressWarnings("unchecked")
    public T get() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int column = r.nextInt(probability.length);
        boolean coinToss = r.nextDouble() < probability[column];
        int index = coinToss ? column : alias[column];
        return (T) values[index];
    }
}
