package ru.zudin.triclustering.model;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class Utils {
    public static void preCheck(int index, int dimension) {
        if (index < 0 || index >= dimension)
            throw new IllegalArgumentException("Illegal index");
    }
}
