package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.hse.zudin.triclustering.model.FormalContext;
import ru.hse.zudin.triclustering.model.Tuple;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 16.05.15.
 */
public class CollectReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        FormalContext formalContext = new FormalContext();
        for (Text value : values) {
            Tuple[] tuples = HadoopIOUtils.parseTuples(value);
            for (Tuple tuple : tuples) {
                formalContext.add(tuple);
            }
        }

        for (Tuple cluster : formalContext.getClusters()) {
            context.write(new LongWritable(0), HadoopIOUtils.asText(cluster));
        }
    }
}
