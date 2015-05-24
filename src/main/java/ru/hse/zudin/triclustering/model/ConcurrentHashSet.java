package ru.hse.zudin.triclustering.model;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Sergey Zudin
 * @since 24.05.15.
 */
public class ConcurrentHashSet<T> extends AbstractSet<T> {
    private Map<T, Boolean> map;
    private Boolean value;

    public ConcurrentHashSet() {
        this.map = new ConcurrentHashMap<>();
        value = true;
    }

    @Override
    public Iterator iterator() {
        return map.keySet().iterator();
    }

    @Override
    public int size() {
        return map.keySet().size();
    }

    @Override
    public boolean add(T o) {
        map.putIfAbsent(o, value);
        return value;
    }
}
