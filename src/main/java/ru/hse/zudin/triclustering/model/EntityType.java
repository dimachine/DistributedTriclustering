package ru.hse.zudin.triclustering.model;

import java.util.Arrays;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public enum EntityType {
    EXTENT,
    INTENT,
    MODUS;

    public static int size() {
        return values().length;
    }

    public static List<EntityType> triclusteringEntities() {
        return Arrays.asList(values()).subList(0, 3);
    }
}
