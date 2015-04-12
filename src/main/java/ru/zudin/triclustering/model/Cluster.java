package ru.zudin.triclustering.model;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.list.FixedSizeList;

import java.util.*;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class Cluster {
    private List<Set<Entity>> entities;

    public Cluster() {
        this.entities = FixedSizeList.fixedSizeList(new ArrayList<>(EntityType.size()));
    }

    public void set(int index, Collection<Entity> collection) {
        Utils.preCheck(index, dimension());
        entities.set(index, new HashSet<>(collection));
    }

    public Set<Entity> get(int index) {
        Utils.preCheck(index, dimension());
        return entities.get(index);
    }

    public int dimension() {
        return entities.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        if (tuple.dimension() != dimension()) return false;
        for (int i = 0; i < dimension(); i++) {
            if (!CollectionUtils.isEqualCollection(get(i), tuple.get(i)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return entities.stream()
                .flatMap(Collection::stream)
                .mapToInt(Object::hashCode)
                .reduce((a, b) -> 31 * a + b)
                .getAsInt();
    }
}
