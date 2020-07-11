package com.biubiu.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum = 0;

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value: values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
