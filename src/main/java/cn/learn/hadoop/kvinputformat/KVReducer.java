package cn.learn.hadoop.kvinputformat;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class KVReducer  extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable sumInt = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable cnt:values) {
            sum+=cnt.get();
        }
        sumInt.set(sum);
        context.write(key,sumInt);

    }
}
