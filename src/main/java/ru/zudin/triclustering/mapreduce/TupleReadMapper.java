package ru.zudin.triclustering.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.zudin.triclustering.model.Entity;
import ru.zudin.triclustering.model.EntityType;
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
    private String mainDelimiter;
    private String insideDelimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        /*
         * TODO: use params
         * conf.get("main_delimiter")
         * conf.get("inside_delimiter")
         */
        mainDelimiter = "\t";
        insideDelimiter = ";";
    }

    @Override
    protected void map(Long key, String value, Context context) throws IOException, InterruptedException {
        String[] data = value.split(mainDelimiter);
        if (data.length != EntityType.size())
            throw new IllegalArgumentException("Dimensions from file and config are different");
        Tuple tuple = new Tuple();
        for (int i = 0; i < data.length; i++) {
            String[] innerData = data[i].split(insideDelimiter);
            final int finalI = i;
            List<Entity<Text>> collected = Arrays.asList(innerData).stream()
                    .map(Text::new)
                    .map(elem -> new Entity<>(elem, EntityType.values()[finalI]))
                    .collect(Collectors.toList());
            tuple.set(i, collected);
        }
        context.write(tuple.hashCode(), tuple);
    }
}
