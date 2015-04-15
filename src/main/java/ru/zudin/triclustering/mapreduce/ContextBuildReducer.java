package ru.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import ru.zudin.triclustering.model.FormalContext;
import ru.zudin.triclustering.model.Tuple;

import java.io.IOException;
import java.util.Set;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class ContextBuildReducer extends Reducer<IntWritable, Tuple, IntWritable, Tuple> {
//    private static final FormalContext formalContext = new FormalContext();

    @Override
    protected void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
        FormalContext formalContext = new FormalContext();
        for (Tuple tuple : values) {
            formalContext.add(tuple);
        }
        Set<Tuple> clusters = formalContext.getClusters();
        for (Tuple cluster : clusters) {
            context.write(new IntWritable(cluster.hashCode()), cluster);
        }
    }
}
