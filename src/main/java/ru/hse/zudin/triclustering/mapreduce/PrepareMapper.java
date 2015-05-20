package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 20.05.15.
 */
public class PrepareMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(0), new Text(value.toString().split("\t", 2)[1]));
    }
}
