package cn.learn.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class WcDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("hadoop.home.dir", "D:\\bigDataInstall\\hadoop-2.7" +
                ".2" );
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WcDriver.class);

        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
