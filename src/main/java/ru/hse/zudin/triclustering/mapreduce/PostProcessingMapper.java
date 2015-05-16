package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.hse.zudin.triclustering.model.Tuple;
import ru.hse.zudin.triclustering.parameters.DensityParameter;
import ru.hse.zudin.triclustering.parameters.Parameter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class PostProcessingMapper extends Mapper<LongWritable, Text, LongWritable, Tuple> {
    private List<Parameter> parameters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        parameters = Arrays.asList(
                new DensityParameter()
        );
    }

    @Override
    protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        Tuple tuple = HadoopIOUtils.parseTuple(text);
        for (Parameter parameter : parameters) {
            if (!parameter.check(tuple)) {
                return;
            }
        }
        context.write(key, tuple);
    }
}
