package ru.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.zudin.triclustering.model.Tuple;
import ru.zudin.triclustering.parameters.DensityParameter;
import ru.zudin.triclustering.parameters.Parameter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class PostProcessingMapper extends Mapper<LongWritable, Tuple, IntWritable, Tuple> {
    private List<Parameter> parameters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        parameters = Arrays.asList(
                new DensityParameter()
        );
    }

    @Override
    protected void map(LongWritable key, Tuple tuple, Context context) throws IOException, InterruptedException {
        for (Parameter parameter : parameters) {
            if (!parameter.check(tuple)) {
                return;
            }
        }
        context.write(new IntWritable(0), tuple);
    }
}
