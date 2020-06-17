package cn.learn.hadoop.mutiplejobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Fiona--a.txt	2
 * 期望输出 atguigu	c.txt-->2	b.txt-->2	a.txt-->3
 */
public class SecondIndexReducer  extends Reducer<Text, Text,Text,Text> {
   Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder newLine = new StringBuilder();
        for (Text value : values) {
            newLine.append(value.toString().replace("\t","-->")+"\t");
        }
        newLine.deleteCharAt(newLine.length() - 1);
        v.set(newLine.toString());
        context.write(key,v);
    }
}
