package com.biubiu.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosamazon;
    FSDataOutputStream fosother;
    public FRecordWriter(TaskAttemptContext job){
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            fosamazon = fs.create(new Path("output/output.log"));
            fosother = fs.create(new Path("output/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.toString().contains("amazon")){
            fosamazon.write(text.toString().getBytes());
        }else{
            fosother.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fosamazon);
        IOUtils.closeStream(fosother);
    }
}
