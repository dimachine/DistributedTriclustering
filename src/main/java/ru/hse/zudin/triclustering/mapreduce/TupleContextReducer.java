package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.hse.zudin.triclustering.model.FormalContext;
import ru.hse.zudin.triclustering.model.Tuple;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class TupleContextReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        FormalContext formalContext = new FormalContext();
        for (Text value : values) {
            Tuple tuple = HadoopIOUtils.parseText(value, Tuple.class, false);
            formalContext.add(tuple);
        }


        for (Tuple tuple : formalContext.getClusters(4)) {
            context.write(new LongWritable(0), HadoopIOUtils.asText(tuple, true));
        }
    }
}
