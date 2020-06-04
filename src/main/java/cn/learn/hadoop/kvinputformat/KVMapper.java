package cn.learn.hadoop.kvinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KVMapper extends Mapper <Text, Text, Text, IntWritable>{
    IntWritable cnt = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,cnt);

    }
}
