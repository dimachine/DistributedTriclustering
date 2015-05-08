package ru.hse.zudin.triclustering.parameters;

import ru.hse.zudin.triclustering.model.Tuple;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public interface Parameter {
    public boolean check(Tuple tuple);
}
