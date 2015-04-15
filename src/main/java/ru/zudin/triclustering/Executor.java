package ru.zudin.triclustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import ru.zudin.triclustering.mapreduce.ChainingJob;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class Executor {
    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        fileSystem.delete(new Path("temp"), true);
        fileSystem.delete(new Path("result"), true);
        ToolRunner.run(new Configuration(), new ChainingJob(), new String[]{ "data/test.txt" , "result" });
    }
}
