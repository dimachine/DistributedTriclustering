package ru.hse.zudin.triclustering.model;

import org.apache.commons.collections4.list.FixedSizeList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class ModelUtils {
    public static void preCheck(int index, int dimension) {
        if (index < 0 || index >= dimension)
            throw new IllegalArgumentException("Illegal index");
    }

    public static <T> List<T> getFixedList(int size) {
        ArrayList<T> list = new ArrayList<>(Collections.nCopies(size, null));
        return FixedSizeList.fixedSizeList(list);
    }
}
