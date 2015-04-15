package ru.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;
import ru.zudin.triclustering.model.FormalContext;
import ru.zudin.triclustering.model.Tuple;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class TupleContextReducer extends Reducer<IntWritable, Tuple, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
        FormalContext formalContext = new FormalContext();
        ObjectMapper mapper = new ObjectMapper();
        for (Tuple tuple : values) {
            formalContext.add(tuple);
        }
        context.write(new IntWritable(0), new Text(mapper.writeValueAsString(formalContext)));
    }
}
