package cn.learn.hadoop.mutiplejobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SecondIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\bigDataInstall\\hadoop-2.7" +
                ".2" );
        args = new String[]{"C:/Users/Administrator/Desktop/Project/Data" +
                "/output/part-r-00000","C:/Users/Administrator/Desktop/Project/Data" +
                "/output1"};
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(SecondIndexDriver.class);
        job.setMapperClass(SecondIndexMapper.class);
        job.setReducerClass(SecondIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
