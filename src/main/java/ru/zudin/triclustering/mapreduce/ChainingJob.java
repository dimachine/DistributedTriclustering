package ru.zudin.triclustering.mapreduce;

import net.jodah.typetools.TypeResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 15.04.15.
 */
public class ChainingJob extends Configured implements Tool {
    private static final String TEMP_PATH = "temp";
    private List<Job> jobs;
    private String name;

    private ChainingJob() {
        jobs = new ArrayList<>();
    }

    private void setUp(String input, String output) throws IOException {
        clear(output);

        Job job = jobs.get(0);
        FileInputFormat.addInputPath(job, new Path(input));
        for (int i = 1; i < jobs.size(); i++) {
            Job job2 = jobs.get(i);
            FileOutputFormat.setOutputPath(job, new Path(TEMP_PATH + i));
            FileInputFormat.addInputPath(job2, new Path(TEMP_PATH + i));
            job = job2;
        }
        FileOutputFormat.setOutputPath(jobs.get(jobs.size() - 1), new Path(output));
    }

    private void clear(String output) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(
                fileSystem.getWorkingDirectory(), true);
        List<Path> paths = new ArrayList<>();
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            Path path = fileStatus.getPath();
            if (path.getName().contains(TEMP_PATH)) {
                paths.add(path);
            }
        }
        for (Path path : paths) {
            fileSystem.delete(path, true);
        }
        fileSystem.delete(new Path(output), true);
    }

    @Override
    public int run(String[] strings) throws Exception {
        setUp(strings[0], strings[1]);
        for (Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                throw new Exception("Job exception");
            }
        }
        return 1;
    }

    public interface NamedBuilder {
        public MapRedBuilder name(String name);
    }

    public interface MapRedBuilder {
        public ReadyBuilder mapper(Class<? extends Mapper> cls) throws IOException;
        public ReadyBuilder reducer(Class<? extends Reducer> cls) throws IOException;
    }

    public interface ReadyBuilder extends MapRedBuilder {
        public ChainingJob build();
    }

    public static class Builder implements NamedBuilder, ReadyBuilder {
        private ChainingJob chainingJob;
        private Job job;
        private boolean isPrevMapper = false;
        private Configuration conf;


        private Builder() {
            chainingJob = new ChainingJob();
            conf = new Configuration();
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
        public ReadyBuilder mapper(Class<? extends Mapper> cls) throws IOException {
            if (isPrevMapper) {
                job.setNumReduceTasks(0);
                chainingJob.jobs.add(job);
            }
            job = Job.getInstance(conf, chainingJob.name);
            job.setJarByClass(ChainingJob.class);
            job.setMapperClass(cls);
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(Mapper.class, cls);
            job.setOutputKeyClass(typeArgs[2]);
            job.setOutputValueClass(typeArgs[3]);
            isPrevMapper = true;
            return this;
        }

        @Override
        public ReadyBuilder reducer(Class<? extends Reducer> cls) throws IOException {
            if (!isPrevMapper) {
                job = Job.getInstance(conf, chainingJob.name);
                job.setJarByClass(ChainingJob.class);
            }
            job.setReducerClass(cls);
            Class<?>[] typeArgs = TypeResolver.resolveRawArguments(Reducer.class, cls);
            job.setOutputKeyClass(typeArgs[2]);
            job.setOutputValueClass(typeArgs[3]);
            chainingJob.jobs.add(job);
            job = null;
            isPrevMapper = false;
            return this;
        }

        @Override
        public ChainingJob build() {
            if (job != null) {
                job.setNumReduceTasks(0);
                chainingJob.jobs.add(job);
            }
            return chainingJob;
        }
    }
}
