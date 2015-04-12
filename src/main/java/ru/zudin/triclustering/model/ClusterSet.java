package ru.zudin.triclustering.model;

import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.commons.collections4.list.FixedSizeList;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class ClusterSet {
    private static final int MAXIMUM_SIZE = 5;
    List<MultiKeyMap> maps;

    public ClusterSet(int dimension) {
        try {
            Utils.preCheck(dimension, MAXIMUM_SIZE);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedSizeExeption("Size < 1 or > 5 is not supported");
        }
        maps = FixedSizeList.fixedSizeList(new ArrayList<>(dimension));
    }

    public void add(int index, Entity value, Entity... keys) {
        Utils.preCheck(index, MAXIMUM_SIZE);
        Utils.preCheck(keys.length, MAXIMUM_SIZE);
        MultiKeyMap map = maps.get(index);
        if (map == null) map = new MultiKeyMap();
        map.put(keys, value);
    }

    public void get(int index, Entity... keys) {
        Utils.preCheck(index, MAXIMUM_SIZE);

    }
}
