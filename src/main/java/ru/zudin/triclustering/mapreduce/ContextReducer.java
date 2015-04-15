package ru.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jackson.map.ObjectMapper;
import ru.zudin.triclustering.model.FormalContext;
import ru.zudin.triclustering.model.Tuple;

import java.io.IOException;
import java.util.Set;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class ContextReducer extends Reducer<LongWritable, Text, LongWritable, Tuple> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        FormalContext formalContext = new FormalContext();
        for (Text text : values) {
            String content = text.toString();
            String[] data = content.split("\t");
            formalContext.add(mapper.readValue(data[1], FormalContext.class));
        }

        Set<Tuple> clusters = formalContext.getClusters();
        for (Tuple cluster : clusters) {
            context.write(new LongWritable(cluster.hashCode()), cluster);
        }
    }
}
