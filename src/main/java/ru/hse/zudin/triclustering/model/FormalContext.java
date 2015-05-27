package ru.hse.zudin.triclustering.model;

import org.apache.log4j.Logger;

import java.util.Set;

/**
 * Formal context for clustering
 *
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class FormalContext {

    private static final Logger logger = Logger.getLogger(FormalContext.class);

//    private ConcurrentHashSet<Tuple> tuples;
    private ConcurrentHashSet<Tuple> clusters;
    public EntityStorage storage;

    /**
     * Base constructor
     */
    public FormalContext() {
//        tuples = new ConcurrentHashSet<>();
        clusters = new ConcurrentHashSet<>();
        storage = new EntityStorage();
    }

    /**
     * Adds a tuple to formal context: into set of all tuples (@see Context#tuples), into cluster storage
     * (@see ClusterConstructor) and its components into set of entities (@see Context#entities)
     * @param tuple tuple to add
     */
    public void add(Tuple tuple) {
        if (tuple.dimension() != EntityType.size())
            throw new IllegalArgumentException("Dimensions are different");
//        int oldSize = tuples.size();
//        tuples.add(tuple);
        Tuple cluster = new Tuple();
//        if (oldSize == tuples.size()) return;
        for (int i = 0; i < EntityType.size(); i++) {
            for (Entity elem : tuple.get(i)) {
                Set<Entity> add = storage.add(elem, tuple.getAllExcept(i));
                cluster.set(i, add);
            }
        }
        clusters.add(cluster);
    }

    /**
     * Returns result set of all existing clusters by using a cluster storage (@see ClusterConstructor)
     * It iterates over tuples and build clusters based on their components.
     * @return set of all existing clusters
     */
    public Set<Tuple> getClusters(int threads) throws InterruptedException {
        return clusters;
//        ExecutorService service = Executors.newFixedThreadPool(threads);
//        Set<Tuple> result = new ConcurrentHashSet<>();
//        AtomicInteger integer = new AtomicInteger();
//        for (Tuple tuple : tuples) {
//            service.submit(new Runnable() {
//                @Override
//                public void run() {
//                    result.add(getCluster(tuple));
//                    integer.incrementAndGet();
//                    if (integer.intValue() % 100 == 0)
//                        logger.info("CREATING CLUSTERS: " + integer.intValue() + " / " + tuples.size());
//                }
//            });
//        }
//        service.shutdown();
//        service.awaitTermination(24, TimeUnit.HOURS);
//        return result;
    }

    private Tuple getCluster(Tuple tuple) {
        Tuple cluster = new Tuple();
        for (int i = 0; i < tuple.dimension(); i++) {
            cluster.set(i, storage.get(tuple.getAllExcept(i)));
        }
        return cluster;
    }
}
