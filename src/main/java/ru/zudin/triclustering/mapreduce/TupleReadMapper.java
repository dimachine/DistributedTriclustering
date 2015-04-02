package ru.zudin.triclustering.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.zudin.triclustering.model.Entity;
import ru.zudin.triclustering.model.Tuple;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Sergey Zudin
 * @since 08.03.15.
 */
public class TupleReadMapper extends Mapper<Long, String, Integer, Tuple> {
    private Tuple.Factory factory;
    private String mainDelimiter;
    private String insideDelimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        /*
         * TODO: use params
         * conf.get("dimension")
         * conf.get("main_delimiter")
         * conf.get("inside_delimiter")
         */
        factory = new Tuple.Factory(3);
        mainDelimiter = "\t";
        insideDelimiter = ";";
    }

    @Override
    protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
        String[] data = value.split(mainDelimiter); //The Shawshank Redemption  (1994)	Prison	Crime
        if (data.length != factory.dimension())
            throw new IllegalArgumentException("Dimensions from file and config are different");
        Tuple tuple = factory.createTuple();
        for (int i = 0; i < data.length; i++) {
            String[] innerData = data[i].split(insideDelimiter);
            List<Entity<Writable>> collected = Arrays.asList(innerData).stream()
                    .map(Text::new)
                    .map(Entity::new)
                    .collect(Collectors.toList());
            tuple.set(i, collected);
        }
        context.write(tuple.hashCode(), tuple);
    }
}
