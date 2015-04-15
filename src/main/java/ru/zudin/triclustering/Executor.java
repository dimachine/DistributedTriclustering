package ru.zudin.triclustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import ru.zudin.triclustering.mapreduce.ChainingJob;
import ru.zudin.triclustering.mapreduce.ContextBuildReducer;
import ru.zudin.triclustering.mapreduce.PostProcessingMapper;
import ru.zudin.triclustering.mapreduce.TupleReadMapper;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class Executor {
    public static void main(String[] args) throws Exception {
        ChainingJob job = ChainingJob.Builder.instance()
                .name("triclustering")
                .mapper(TupleReadMapper.class)
                .reducer(ContextBuildReducer.class)
                .mapper(PostProcessingMapper.class)
                .build();
        ToolRunner.run(new Configuration(), job, new String[]{ "data/test.txt" , "result" });
    }
}
