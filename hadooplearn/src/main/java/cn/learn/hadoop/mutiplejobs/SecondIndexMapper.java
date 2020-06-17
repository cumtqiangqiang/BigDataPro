package cn.learn.hadoop.mutiplejobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Fiona--a.txt	2
 * 期望输出 atguigu	c.txt-->2	b.txt-->2	a.txt-->3
 */
public class SecondIndexMapper extends Mapper<LongWritable, Text, Text,
        Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("--");

        k.set(fields[0]);
        v.set(fields[1]);
        context.write(k, v);

    }
}
