package ru.zudin.triclustering.model;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.list.FixedSizeList;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class Tuple implements Writable {
    private List<Set<Entity>> entities;

    private Tuple(int capacity) {
        this.entities = FixedSizeList.fixedSizeList(new ArrayList<>(capacity));
    }

    public void set(int index, Collection<Entity> collection) {
        preCheck(index);
        entities.set(index, new HashSet<>(collection));
    }

    public Set<Entity> get(int index) {
        preCheck(index);
        return entities.get(index);
    }

    public int dimension() {
        return entities.size();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(dimension());
        for (Set<Entity> set : entities) {
            output.writeInt(set.size());
            for (Entity entity : set) {
                entity.write(output);
            }
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int capacity = input.readInt();
        entities = FixedSizeList.fixedSizeList(new ArrayList<>(capacity));
        for (int i = 0; i < capacity; i++) {
            Set<Entity> entitySet = new HashSet<>();
            int amount = input.readInt();
            for (int j = 0; j < amount; j++) {
                Entity entity = new Entity();
                entity.readFields(input);
                entitySet.add(entity);
            }
            entities.set(i, entitySet);
        }
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

    private void preCheck(int index) {
        if (index < 0 || index >= dimension())
            throw new IllegalArgumentException("Illegal index");
    }

    public static class Factory {
        private int capacity;

        public Factory(int capacity) {
            this.capacity = capacity;
        }

        public Tuple buildTuple() {
            return new Tuple(capacity);
        }
    }
}
