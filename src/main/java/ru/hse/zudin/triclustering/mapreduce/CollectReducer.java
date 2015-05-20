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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        ExecutorService service = Executors.newFixedThreadPool(32);
        for (Text value1 : values) {
            Text value = new Text(value1);
            service.submit(new Runnable() {
                @Override
                public void run() {
                    Tuple tuple = null;
                    try {
                        tuple = HadoopIOUtils.parseText(value, Tuple.class, true);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    formalContext.add(tuple);
                }
            });
        }
        service.shutdown();
        service.awaitTermination(24, TimeUnit.HOURS);

        for (Tuple cluster : formalContext.getClusters()) {
            for (Parameter parameter : parameters) {
                if (parameter.check(cluster))
                    context.write(new LongWritable(0), new Text(cluster.toString()));
            }
        }
    }
}
