package ru.hse.zudin.triclustering.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import ru.hse.zudin.triclustering.model.FormalContext;
import ru.hse.zudin.triclustering.model.Tuple;
import ru.hse.zudin.triclustering.parameters.DensityParameter;
import ru.hse.zudin.triclustering.parameters.Parameter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Sergey Zudin
 * @since 16.05.15.
 */
public class CollectReducer extends Reducer<LongWritable, Tuple, LongWritable, Text> {

    private static final Logger logger = Logger.getLogger(CollectReducer.class);

    private List<Parameter> parameters = new ArrayList<>();
    private int threads;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        parameters.add(new DensityParameter());
        Configuration conf = context.getConfiguration();
        threads = Integer.parseInt(conf.get(Constants.THREADS));
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Tuple> tuples, Context context) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        FormalContext formalContext = new FormalContext();
        ExecutorService service = Executors.newFixedThreadPool(threads);
        AtomicInteger integer = new AtomicInteger();
        AtomicInteger integer2 = new AtomicInteger();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                logger.info("create: " + formalContext.storage.create + "; add: " + formalContext.storage.add);
                formalContext.storage.create.set(0);
                formalContext.storage.add.set(0);
            }
        }, 0, 2000);

        for (Tuple value : tuples) {
            Tuple tuple = new Tuple(value);
            integer.incrementAndGet();
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        formalContext.add(tuple);
                        integer2.incrementAndGet();
                        if (integer2.intValue() % 100 == 0)
                            logger.info("ADDING: " + integer2.intValue() + " / " + integer.intValue());
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            });
        }
        service.shutdown();
        long second = System.currentTimeMillis();
        service.awaitTermination(24, TimeUnit.HOURS);
        timer.cancel();

        long third = System.currentTimeMillis();
        Set<Tuple> clusters = formalContext.getClusters(threads);
        long forth = System.currentTimeMillis();
        System.out.println("CREATING THREADS: " + TimeUnit.MILLISECONDS.toSeconds(second - start));
        System.out.println("THREADS AWAITING: " + TimeUnit.MILLISECONDS.toSeconds(third - second));
        System.out.println("CLUSTERS GENERATING: " + TimeUnit.MILLISECONDS.toSeconds(forth - third));
        for (Tuple cluster : clusters) {
            for (Parameter parameter : parameters) {
                if (parameter.check(cluster))
                    context.write(new LongWritable(0), new Text(cluster.toString()));
            }
        }
        long finish = System.currentTimeMillis();

        System.out.println("WRITING ON THE DISK: " + TimeUnit.MILLISECONDS.toSeconds(finish - forth));
        System.out.println("TOTAL REDUCER WORKING TIME: " + TimeUnit.MILLISECONDS.toSeconds(finish - start));
    }

    private static List<Tuple> makeCollection(Iterable<Tuple> iter) {
        List<Tuple> list = new ArrayList<>();
        for (Tuple item : iter) {
            list.add(new Tuple(item));
        }
        return list;
    }
}
