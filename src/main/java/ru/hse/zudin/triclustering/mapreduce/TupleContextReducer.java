package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;
import ru.hse.zudin.triclustering.model.FormalContext;
import ru.hse.zudin.triclustering.model.Tuple;

import java.io.IOException;
import java.util.Set;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class TupleContextReducer extends Reducer<IntWritable, Tuple, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
        FormalContext formalContext = new FormalContext();
        ObjectMapper mapper = new ObjectMapper();
        while (values.iterator().hasNext()) {
            Tuple tuple = new Tuple(values.iterator().next().getEntities());
            formalContext.add(tuple);
        }
        Set<Tuple> clusters = formalContext.getClusters();
        for (Tuple cluster : clusters) {
            context.write(new IntWritable(0), new Text(mapper.writeValueAsString(cluster)));
        }
    }
}
