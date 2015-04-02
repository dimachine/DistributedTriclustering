package ru.zudin.triclustering.mapreduce;


import org.apache.hadoop.mapreduce.Mapper;
import ru.zudin.triclustering.model.Tuple;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 08.03.15.
 */
public class TupleReadMapper extends Mapper<Long, String, Long, Tuple> {
    @Override
    protected void map(Long key, String value, Context context) throws IOException, InterruptedException {

        //context.write(1l, "test");
    }
}
