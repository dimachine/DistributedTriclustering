package ru.zudin.triclustering.model;

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
    private int position;

    private Tuple(int capacity) {
        this.entities = FixedSizeList.fixedSizeList(new ArrayList<>(capacity));
        position = 0;
    }

    public void add(Collection<Entity> collection) {
        if (position >= entities.size())
            throw new IndexOutOfBoundsException("List is full");
        Set<Entity> entitySet = new HashSet<>();
        entitySet.addAll(collection);
        entities.set(position++, entitySet);
    }

    public Set<Entity> get(int index) {
        if (index < 0 || index >= entities.size())
            throw new IllegalArgumentException("Illegal index");
        return entities.get(index);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(entities.size());
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
