package ru.zudin.triclustering.model;

import java.util.Set;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public interface ClusterSet {
    public void add(Entity value, Entity... keys);

    public Set<Entity> get(Entity... keys);
}
