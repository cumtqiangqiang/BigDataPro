package cn.learn.hadoop.mutiplejobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 原数据 atguigu  qiang   Fiona
 * 期望输出 China--b.txt	2
 * China--c.txt	2
 * Fiona--a.txt	2
 */
public class OneIndexMapper extends Mapper<LongWritable, Text, Text,
        IntWritable> {
    String fileName;
    IntWritable cnt = new IntWritable(1);
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
         fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
        String[] fields = line.split(" ");

        for (String world: fields) {
            k.set(world+"--"+fileName);
            context.write(k,cnt);
        }


    }
}
