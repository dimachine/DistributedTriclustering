package ru.zudin.triclustering.model;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.list.FixedSizeList;
import org.apache.commons.collections4.map.MultiKeyMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class TriclusterSet implements ClusterSet {
    private static final int SIZE = 3;
    List<MultiKeyMap<Entity, Set<Entity>>> maps;
    private List<EntityType> entities;

    public TriclusterSet() {
        maps = FixedSizeList.fixedSizeList(new ArrayList<>(SIZE));
        entities = EntityType.triclusteringEntities();
    }

    @Override
    public void add(Entity value, Entity... keys) {
        if (keys.length == SIZE - 1) {
            if (keys[0].getType().equals(keys[1].getType()))
                throw new IllegalArgumentException("There are no exact type for search");
            addForPair(value, keys[0], keys[1]);
        } else {
            Multimap<EntityType, Entity> index = Multimaps.index(Arrays.asList(keys), Entity::getType);
            if (index.keySet().size() != SIZE - 1)
                throw new IllegalArgumentException("There are no exact type for search");
            for (Entity key1 : index.get(keys[0].getType())) {
                for (Entity key2 : index.get(keys[keys.length - 1].getType())) {
                    addForPair(value, key1, key2);
                }
            }
        }
    }

    private void addForPair(Entity value, Entity key1, Entity key2) {
        MultiKeyMap<Entity, Set<Entity>> map = getMapByType(value.getType());
        Set<Entity> set = map.get(key1, key2);
        if (set == null) set = new HashSet<>();
        set.add(value);
        map.put(key1, key2, set);
    }

    @Override
    public Set<Entity> get(Entity... keys) {
        if (keys.length == SIZE - 1) {
            if (keys[0].getType().equals(keys[1].getType()))
                throw new IllegalArgumentException("There are no exact type for search");
            return getForPair(keys[0], keys[1]);
        } else {
            Multimap<EntityType, Entity> index = Multimaps.index(Arrays.asList(keys), Entity::getType);
            if (index.keySet().size() != SIZE - 1)
                throw new IllegalArgumentException("There are no exact type for search");
            Set<Entity> result = new HashSet<>();
            for (Entity key1 : index.get(keys[0].getType())) {
                for (Entity key2 : index.get(keys[keys.length - 1].getType())) {
                    result.addAll(getForPair(key1, key2));
                }
            }
            return result;
        }
    }

    private Set<Entity> getForPair(Entity key1, Entity key2) {
        List<EntityType> keyTypes = Arrays.asList(key1, key2).stream().map(Entity::getType).collect(Collectors.toList());
        Collection<EntityType> types = CollectionUtils.removeAll(entities, keyTypes);
        if (types.size() != 1) throw new IllegalArgumentException("There are no exact type for search");
        MultiKeyMap<Entity, Set<Entity>> map = getMapByType(types.iterator().next());
        return map.get(key1, key2);
    }

    private MultiKeyMap<Entity, Set<Entity>> getMapByType(EntityType type) {
        return maps.get(EntityType.triclusteringEntities().indexOf(type));
    }
}
