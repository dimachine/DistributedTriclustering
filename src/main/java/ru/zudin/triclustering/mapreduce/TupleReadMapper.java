package ru.zudin.triclustering.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import ru.zudin.triclustering.Executor;
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
public class TupleReadMapper extends Mapper<LongWritable, Text, IntWritable, Tuple> {
    public static Logger logger = Logger.getLogger(TupleReadMapper.class);
    private String mainDelimiter;
    private String insideDelimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        mainDelimiter = conf.get(Executor.mainDelimiter);
        insideDelimiter = conf.get(Executor.secondaryDelimiter);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(mainDelimiter);
        if (data.length != EntityType.size()) {
            logger.error("Wrong input " + value.toString());
            return;
        }
        Tuple tuple = new Tuple();
        for (int i = 0; i < data.length; i++) {
            String[] innerData = data[i].split(insideDelimiter);
            final int finalI = i;
            List<Entity> collected = Arrays.asList(innerData).stream()
                    .map(Text::new)
                    .map(elem -> new Entity(elem, EntityType.values()[finalI]))
                    .collect(Collectors.toList());
            tuple.set(i, collected);
        }
        context.write(new IntWritable(0), tuple);
    }
}
