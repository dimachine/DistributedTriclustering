package ru.hse.zudin.triclustering.model;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class Tuple implements Writable {
    private List<Set<Entity>> entities;

    public Tuple() {
        this.entities = Utils.getFixedList(dimension());
    }

    public Tuple(List<Set<Entity>> entities) {
        this.entities = entities;
    }

    public Tuple(Tuple tuple) {
        this(tuple.getEntities());
    }

    public List<Set<Entity>> getEntities() {
        return entities;
    }

    public void setEntities(List<Set<Entity>> entities) {
        this.entities = entities;
    }

    public void set(int index, Collection<Entity> collection) {
        Utils.preCheck(index, dimension());
        entities.set(index, new HashSet<>(collection));
    }

    public Set<Entity> get(int index) {
        Utils.preCheck(index, dimension());
        return entities.get(index);
    }

    public List<Entity> getAllExcept(int index) {
        Utils.preCheck(index, dimension());
        return entities.stream()
                .filter(set -> entities.indexOf(set) != index)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public int dimension() {
        return EntityType.size();
    }

    // Hadoop methods

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
        entities = Utils.getFixedList(capacity);
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

    // Storage methods

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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Tuple {");
        for (Set<Entity> set : entities) {
            builder.append("{");
            for (Entity entity : set) {
                builder.append("{").append(entity.getValue()).append(",").append(entity.getType())
                        .append(",").append(entity.getDescription()).append("},");
            }
            builder.deleteCharAt(builder.length()-1);
            builder.append("},");
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append("}");
        return builder.toString();
    }
}
