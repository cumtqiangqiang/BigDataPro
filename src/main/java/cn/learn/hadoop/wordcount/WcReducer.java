package cn.learn.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable count = new IntWritable();
    protected void reduce(Text key, Iterable<IntWritable> values, Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value: values) {
            sum += value.get();
        }
        this.count.set(sum);
        context.write(key, this.count);

    }
}
