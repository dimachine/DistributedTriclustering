package ru.hse.zudin.triclustering.model;

/**
 * @author Sergey Zudin
 * @since 24.05.15.
 */
public class Precluster {
    private ConcurrentHashSet<ConcurrentHashSet<Entity>> set1;
    private ConcurrentHashSet<ConcurrentHashSet<Entity>> set2;
    private ConcurrentHashSet<ConcurrentHashSet<Entity>> set3;

    public Precluster() {
        set1 = new ConcurrentHashSet<>();
        set2 = new ConcurrentHashSet<>();
        set3 = new ConcurrentHashSet<>();
    }

    public void add(ConcurrentHashSet<Entity> set, int index) {

    }
}
