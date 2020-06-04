package cn.learn.hadoop.udfinputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义inputformat 将小文件合成一个文件
 */
public class WholeInputFormat extends FileInputFormat {
    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split,context);
        return recordReader;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
