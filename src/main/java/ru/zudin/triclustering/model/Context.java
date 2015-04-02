package ru.zudin.triclustering.model;

import java.util.List;
import java.util.Set;

/**
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class Context {
    Set<Tuple> tuples;
    List<Set<Entity>> rawData;
}
