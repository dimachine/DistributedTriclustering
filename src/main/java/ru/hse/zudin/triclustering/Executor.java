package ru.hse.zudin.triclustering;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import ru.hse.zudin.triclustering.mapreduce.ChainingJob;
import ru.hse.zudin.triclustering.mapreduce.PostProcessingMapper;
import ru.hse.zudin.triclustering.mapreduce.TupleContextReducer;
import ru.hse.zudin.triclustering.mapreduce.TupleReadMapper;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class Executor {
    public static final String mainDelimiter = "main_delimiter";
    public static final String secondaryDelimiter = "secondary_delimiter";

    private static final String TEMP_DIR = "job_temp";

    public static void main(String[] args) throws Exception {
        String[] params = {
                "data/test.txt", //path to file
                "\t", // main delimiter
                ";" //secondary delimiter
        };
        if (args.length != 0) {
            for (int i = 0; i < args.length && i < params.length; i++) {
                params[i] = args[i];
            }
        }
        BasicConfigurator.configure();

        String output = "result";
        clear(output);
        ChainingJob job = ChainingJob.Builder.instance()
                .name("triclustering")
                .tempDir(TEMP_DIR)
                .mapper(TupleReadMapper.class, ImmutableMap.of(mainDelimiter, params[1],
                        secondaryDelimiter, params[2]))
                .reducer(TupleContextReducer.class)
                .mapper(PostProcessingMapper.class)
                .build();
        ToolRunner.run(new Configuration(), job, new String[]{ params[0] , output } );
    }

    private static void clear(String output) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FileStatus[] fileStatusListIterator = fileSystem.listStatus(fileSystem.getWorkingDirectory(),
                path -> path.toUri().getPath().contains(TEMP_DIR));
        for (FileStatus path : fileStatusListIterator) {
            fileSystem.delete(path.getPath(), true);
        }
        fileSystem.delete(new Path(output), true);
    }
}
