package ru.zudin.triclustering.model;

import org.apache.commons.collections4.list.FixedSizeList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class Context {
    Set<Tuple> tuples;
    List<Set<Entity>> rawData;
    ClusterSet clusters;

    public Context(int dimension) {
        clusters = new ClusterSet(dimension);
        rawData = FixedSizeList.fixedSizeList(new ArrayList<>(dimension));
        for (int i = 0; i < dimension; i++) {
            rawData.set(i, new HashSet<>());
        }
    }

    public int dimension() {
        return rawData.size();
    }

    public void add(Tuple tuple) {
        if (tuple.dimension() != dimension())
            throw new IllegalArgumentException("Dimensions are different");
        tuples.add(tuple);
        for (int i = 0; i < dimension(); i++) {
            rawData.get(i).addAll(tuple.get(i));
        }
    }

}
