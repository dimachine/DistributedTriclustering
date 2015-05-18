package ru.hse.zudin.triclustering;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import ru.hse.zudin.triclustering.mapreduce.*;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class Executor {
    public static void main(String[] args) throws Exception {
        String[] params = {
            "data/test.txt", //path to file
                    "\t", // main delimiter
                    ";", //secondary delimiter
                    "4" //number of reducers
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
                .tempDir(Constants.TEMP_DIR)
                .mapper(TupleReadMapper.class, ImmutableMap.of(Constants.MAIN_DELIMETER, params[1],
                        Constants.SECONDARY_DELIMETER, params[2],
                        Constants.NUM_OF_REDUCERS, params[3]))
                .reducer(TupleContextReducer.class)
                .reducer(CollectReducer.class)
                .build();
        job.getJob(0).setNumReduceTasks(Integer.parseInt(params[3]));
        ToolRunner.run(new Configuration(), job, new String[]{ params[0] , output } );
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
