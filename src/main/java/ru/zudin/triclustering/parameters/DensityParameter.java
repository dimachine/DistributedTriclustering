package ru.zudin.triclustering.parameters;

import ru.zudin.triclustering.model.Tuple;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class DensityParameter implements Parameter {
    private static final double THRESHOLD = 0;

    @Override
    public boolean check(Tuple tuple) {
        return computeThreshold(tuple) > THRESHOLD;
    }

    private double computeThreshold(Tuple tuple) {
        return 1;
    }
}
