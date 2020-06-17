package cn.learn.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable v = new IntWritable(1);
    private Text w = new Text();
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(" ");

        for (String word : words) {
            this.w.set(word);
            context.write(this.w,this.v);
        }


    }
}
