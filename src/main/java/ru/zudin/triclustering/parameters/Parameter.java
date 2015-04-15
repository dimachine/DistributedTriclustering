package ru.zudin.triclustering.parameters;

import ru.zudin.triclustering.model.Tuple;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public interface Parameter {
    public boolean check(Tuple tuple);
}
