package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import ru.hse.zudin.triclustering.model.Tuple;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 20.05.15.
 */
public class PrepareMapper extends Mapper<LongWritable, Text, LongWritable, Tuple> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Tuple tuple = HadoopIOUtils.parseText(value, Tuple.class, true);
        context.write(new LongWritable(0), tuple);
    }
}
