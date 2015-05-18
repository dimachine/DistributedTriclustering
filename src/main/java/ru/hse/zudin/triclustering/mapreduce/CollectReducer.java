package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.hse.zudin.triclustering.model.FormalContext;
import ru.hse.zudin.triclustering.model.Tuple;
import ru.hse.zudin.triclustering.parameters.DensityParameter;
import ru.hse.zudin.triclustering.parameters.Parameter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 16.05.15.
 */
public class CollectReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    private List<Parameter> parameters = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        parameters.add(new DensityParameter());
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        FormalContext formalContext = new FormalContext();
        for (Text value : values) {
            Tuple[] tuples = HadoopIOUtils.parseText(value, Tuple[].class, true);
            for (Tuple tuple : tuples) {
                formalContext.add(tuple);
            }
        }

        for (Tuple cluster : formalContext.getClusters()) {
            for (Parameter parameter : parameters) {
                if (parameter.check(cluster))
                    context.write(new LongWritable(0), new Text(cluster.toString()));
            }
        }
    }
}
