package cn.learn.hadoop.kvinputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\bigDataInstall\\hadoop-2.7" +
                ".2" );
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);
        job.setJarByClass(KVDriver.class);

        job.setMapperClass(KVMapper.class);
        job.setReducerClass(KVReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
