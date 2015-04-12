package ru.zudin.triclustering.model;

import java.util.Set;

/**
 * Base interface for result set of clusters. May be used future implementation
 *
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public interface ClusterConstructor {
    public void add(Entity value, Entity... keys);
    public Set<Entity> get(Entity... keys);
}
