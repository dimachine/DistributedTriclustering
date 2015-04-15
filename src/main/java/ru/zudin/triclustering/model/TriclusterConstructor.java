package ru.zudin.triclustering.model;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.MultiKeyMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Implementation of {@link ru.zudin.triclustering.model.ClusterConstructor} class for triclustering
 * Works only with tuples with 3 components, which, in this case, are called extent, intent and modus.
 *
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class TriclusterConstructor implements ClusterConstructor {
    private static final int SIZE = 3;
    private List<MultiKeyMap<Entity, Set<Entity>>> maps;
    private List<EntityType> entities;

    /**
     * Base constructor
     */
    public TriclusterConstructor() {
        maps = Utils.getFixedList(SIZE);
        for (int i = 0; i < SIZE; i++) {
            maps.set(i, new MultiKeyMap<>());
        }
        entities = EntityType.triclusteringEntities();
    }

    /**
     * Add value with givan keys. If there 2 keys, just add given value into MultiKeyMap
     * (@see org.apache.commons.collections4.map.MultiKeyMap) with given keys. If there more than 2 keys,
     * it add given value for each pair of keys with different type (@see EntityType).
     *
     * Note that order of keys is important. Entities with the same type have to placed together, for example
     * "(EXTENT, EXTENT, MODUS, MODUS, MODUS)". Elements with the first type ("EXTENT" in example below)
     * in a row are always used as a first key.
     * @param value value to add
     * @param keys keys for map
     */
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

    /**
     * Adds value for given pair of keys. Map to adding is selected by type of value
     * @param value value to add
     * @param key1 first key
     * @param key2 second key
     */
    private void addForPair(Entity value, Entity key1, Entity key2) {
        MultiKeyMap<Entity, Set<Entity>> map = getMapByType(value.getType());
        Set<Entity> set = map.get(key1, key2);
        if (set == null) set = new HashSet<>();
        set.add(value);
        map.put(key1, key2, set);
    }

    /**
     * Returns a set of entites for given keys.
     * @param keys
     * @return
     */
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
            Set<Entity> result = null;
            for (Entity key1 : index.get(keys[0].getType())) {
                for (Entity key2 : index.get(keys[keys.length - 1].getType())) {
                    Set<Entity> forPair = getForPair(key1, key2);
                    result = result == null ? forPair : new HashSet<>(CollectionUtils.intersection(result, forPair));
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
