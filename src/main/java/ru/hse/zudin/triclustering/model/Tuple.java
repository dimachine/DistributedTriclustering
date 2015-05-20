package ru.hse.zudin.triclustering.model;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class Tuple {
    private List<Set<Entity>> entities;

    public Tuple() {
        this.entities = ModelUtils.getFixedList(dimension());
    }

    public Tuple(List<Set<Entity>> entities) {
        this.entities = entities;
    }

    public List<Set<Entity>> getEntities() {
        return entities;
    }

    public void setEntities(List<Set<Entity>> entities) {
        this.entities = entities;
    }

    public void set(int index, Collection<Entity> collection) {
        ModelUtils.preCheck(index, dimension());
        entities.set(index, new HashSet<>(collection));
    }

    public Set<Entity> get(int index) {
        ModelUtils.preCheck(index, dimension());
        return entities.get(index);
    }

    public Entity[][] getAllExcept(int index) {
        Entity[][] result = new Entity[entities.size() - 1][];
        ModelUtils.preCheck(index, dimension());
        for (int i = 0, j = 0; i < entities.size(); i++) {
            if (i == index) continue;
            Set<Entity> set = entities.get(i);
            Entity[] array = set.toArray(new Entity[set.size()]);
            result[j++] = array;
        }
        return result;
    }

    public int dimension() {
        return EntityType.size();
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
                .reduce((a, b) -> a + b)
                .getAsInt();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Tuple {");
        for (Set<Entity> set : entities) {
            builder.append("{");
            for (Entity entity : set) {
                builder.append("{").append(entity.getValue()).append(",").append(entity.getType())
                        .append("},");
            }
            builder.deleteCharAt(builder.length()-1);
            builder.append("},");
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append("}");
        return builder.toString();
    }
}
