package ru.hse.zudin.triclustering.model;

import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Class for tricluster building and storing
 * Works only with tuples with 3 components, which, in this case, are called extent, intent and modus.
 *
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class EntityStorage {
    private Map<MultiKey, Queue<Entity>> map1;
    private Map<MultiKey, Queue<Entity>> map2;
    private Map<MultiKey, Queue<Entity>> map3;
    private Queue<EntityType> types;

    /**
     * Base constructor
     */
    public EntityStorage() {
        types = new ConcurrentLinkedQueue<>(EntityType.triclusteringEntities());
        map1 = new ConcurrentHashMap<>();
        map2 = new ConcurrentHashMap<>();
        map3 = new ConcurrentHashMap<>();
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
    public void add(Entity value, Entity[]... keys) {
        check(keys);
        for (Entity first : keys[0]) {
            for (Entity second : keys[1]) {
                add(new MultiKey(first, second), value);
            }
        }
    }

    private void check(Entity[][] keys) {
        if (keys == null || keys.length != 2
                || keys[0] == null || keys[0].length == 0
                || keys[1] == null || keys[1].length == 0)
            throw new IllegalArgumentException("Size is incorrect");
    }

    /**
     * Adds value for given pair of keys. Map to adding is selected by type of value
     * @param value value to add
     */
    public void add(MultiKey key, Entity value) {
        boolean isNew = false;
        Map<MultiKey, Queue<Entity>> map = getMap(value.getType());


        Queue<Entity> queue = map.get(key);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<>();
            isNew = true;
        }
        queue.add(value);
        if (isNew) map.put(key, queue);
    }

    private Map<MultiKey, Queue<Entity>> getMap(EntityType type) {
        switch (EntityType.triclusteringEntities().indexOf(type)) {
            case 0:
                return map1;
            case 1:
                return map2;
            case 2:
                return map3;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Returns a set of entites for given keys.
     * @param keys
     * @return
     */
    public Set<Entity> get(Entity[]... keys) {
        check(keys);
        Set<Entity> entities = new HashSet<>();
        for (Entity first : keys[0]) {
            for (Entity second : keys[1]) {
                entities.addAll(get(new MultiKey(first, second)));
            }
        }
        return entities;
    }

    private Queue<Entity> get(MultiKey key) {
        List<EntityType> subtract = (List<EntityType>) CollectionUtils.subtract(types, key.types);
        return getMap(subtract.get(0)).get(key);
    }

    public static class MultiKey {
        private List<Entity> keys;
        private Set<EntityType> types;

        public MultiKey(Entity... keys) {
            this.keys = new ArrayList<>();
            this.keys.addAll(Arrays.asList(keys));
            types = new HashSet<>();
            for (Entity key : keys) {
                types.add(key.getType());
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MultiKey multiKey = (MultiKey) o;

            if (multiKey.keys.size() != keys.size()) return false;

            for (Entity key : keys) {
                if (!multiKey.keys.contains(key)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 31;
            for (Entity key : keys) {
                hash += key.hashCode();
            }
            return hash;
        }
    }
}
