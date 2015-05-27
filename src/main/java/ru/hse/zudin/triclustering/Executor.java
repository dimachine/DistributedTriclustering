package ru.hse.zudin.triclustering;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import ru.hse.zudin.triclustering.mapreduce.*;
import ru.zudin.ChainingJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
@Parameters(separators = "=")
public class Executor {

    @Parameter
    public List<String> parameters = new ArrayList<>();

    @Parameter(names = { "-md", "--main_delim" }, description = "Main delimeter")
    public String mainDelimeter = "\t";

    @Parameter(names = { "-sd", "--second_delim" }, description = "Secondary delimeter")
    public String secondaryDelimeter = "¥";

    @Parameter(names = { "-rd", "--reducers" }, description = "Number of reducers")
    public int reducers = 5;

    @Parameter(names = { "-th", "--threads" }, description = "Number of threads")
    public int threads = 5;

    @Parameter(names = { "-out", "--output" }, description = "Output dir")
    public String output = "result";

    @Parameter(names = { "-g", "--group" }, description = "Group index")
    public int groupIndex = 0;

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Executor executor = new Executor();
        new JCommander(executor, args);
        if (executor.mainDelimeter.length() == 0) executor.mainDelimeter = " ";

//        BasicConfigurator.configure();
        clear(executor.output);

        ChainingJob job = ChainingJob.Builder.instance()
                .name("triclustering")
                .tempDir(Constants.TEMP_DIR)
                .mapper(TupleReadMapper.class, ImmutableMap.of(Constants.MAIN_DELIMETER, executor.mainDelimeter,
                        Constants.SECONDARY_DELIMETER, executor.secondaryDelimeter,
                        Constants.NUM_OF_REDUCERS, Integer.toString(executor.reducers),
                        Constants.GROUP_INDEX, Integer.toString(executor.groupIndex)))
                .reducer(TupleContextReducer.class)
                .mapper(PrepareMapper.class)
                .reducer(CollectReducer.class, ImmutableMap.of(Constants.THREADS, Integer.toString(executor.threads)))
                .build();
        job.getJob(0).setNumReduceTasks(executor.reducers);
        job.getJob(1).setNumReduceTasks(1);

        ToolRunner.run(new Configuration(), job, new String[]{executor.parameters.get(0), executor.output});

        long elapsed = System.currentTimeMillis() - start;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(elapsed);
        Logger.getLogger(Executor.class).info("TOTAL TIME: " + seconds);
    }

    private static void clear(String output) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FileStatus[] fileStatusListIterator = fileSystem.listStatus(fileSystem.getWorkingDirectory(),
                path -> path.toUri().getPath().contains(Constants.TEMP_DIR));
        for (FileStatus path : fileStatusListIterator) {
            fileSystem.delete(path.getPath(), true);
        }
        fileSystem.delete(new Path(output), true);
    }
}
