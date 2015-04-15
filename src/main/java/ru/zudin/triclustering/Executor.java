package ru.zudin.triclustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import ru.zudin.triclustering.mapreduce.ChainingJob;
import ru.zudin.triclustering.mapreduce.PostProcessingMapper;
import ru.zudin.triclustering.mapreduce.TupleContextReducer;
import ru.zudin.triclustering.mapreduce.TupleReadMapper;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class Executor {
    private static final String TEMP_DIR = "job_temp";

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        String output = "result";
        clear(output);
        ChainingJob job = ChainingJob.Builder.instance()
                .name("triclustering")
                .tempDir(TEMP_DIR)
                .mapper(TupleReadMapper.class)
                .reducer(TupleContextReducer.class)
                .mapper(PostProcessingMapper.class)
                .build();
        ToolRunner.run(new Configuration(), job, new String[]{ "data/imdb.txt" , output } );
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
