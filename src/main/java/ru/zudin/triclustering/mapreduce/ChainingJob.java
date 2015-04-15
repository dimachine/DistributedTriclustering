package ru.zudin.triclustering.mapreduce;

import net.jodah.typetools.TypeResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import ru.zudin.triclustering.model.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class ChainingJob extends Configured implements Tool {

    List<Job> jobs;
    String name;

    private ChainingJob() {
        jobs = new ArrayList<>();
    }

    @Override
    public int run(String[] strings) throws Exception {
        /*
        * Job 1
        */

        String pathMiddle = "temp";



        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "triclustering");
        job.setJarByClass(ChainingJob.class);

        Class<TupleReadMapper> mapperClass = TupleReadMapper.class;
        Class<ContextBuildReducer> reducerClass = ContextBuildReducer.class;

        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);

        Class<?>[] typeArgs = TypeResolver.resolveRawArguments(Reducer.class, reducerClass);

        job.setOutputKeyClass(typeArgs[2]);
        job.setOutputValueClass(typeArgs[3]);


        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(pathMiddle));
        if (!job.waitForCompletion(true)) {
            throw new Exception("Job exception");
        }



      /*
       * Job 2
       */
        Configuration conf2 = getConf();
        Job job2 = new Job(conf2, "Job 2");
        job2.setJarByClass(ChainingJob.class);

        job2.setMapperClass(PostProcessingMapper.class);
        job2.setNumReduceTasks(0);

        job2.setOutputKeyClass(Integer.class);
        job2.setOutputValueClass(Tuple.class);

        FileInputFormat.addInputPath(job2, new Path(pathMiddle));
//        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job2, new Path(strings[1]));

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    interface NamedBuilder {
        public MapRedBuilder name(String name);
    }

    interface MapRedBuilder {
        public ReadyBuilder mapper(Class<? extends Mapper> cls);
        public ReadyBuilder reducer(Class<? extends Reducer> cls);
    }

    interface ReadyBuilder extends MapRedBuilder {
        public ChainingJob build();
    }

    public static class Builder implements NamedBuilder, ReadyBuilder {
        private ChainingJob chainingJob;

        private Builder() {
            chainingJob = new ChainingJob();
        }

        public static NamedBuilder instance() {
            return new Builder();
        }

        @Override
        public MapRedBuilder name(String name) {
            chainingJob.name = name;
            return this;
        }

        @Override
        public ReadyBuilder mapper(Class<? extends Mapper> cls) {
            return null;
        }

        @Override
        public ReadyBuilder reducer(Class<? extends Reducer> cls) {
            return null;
        }

        @Override
        public ChainingJob build() {
            return null;
        }
    }
}
