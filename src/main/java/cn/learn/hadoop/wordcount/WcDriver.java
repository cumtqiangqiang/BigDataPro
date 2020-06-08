package cn.learn.hadoop.wordcount;

import com.sun.xml.bind.v2.runtime.reflect.opt.Const;
import common.utils.ConfigurationManager;
import common.utils.Constant;
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

//        if (ConfigurationManager.getBoolean(Constant.HADOOP_RUN_LOCAL)){
//            if (ConfigurationManager.getBoolean(Constant.COMPANY_RUN)){
                System.setProperty("hadoop.home.dir", Constant.HADOOP_COMPANY_PATH);
                args = new String[] { "C:\\Users\\UC227911\\Desktop\\Pro\\testData\\input\\word.txt",
                        Constant.COMPANY_OUTPUT};
//            }else {
//                System.setProperty("hadoop.home.dir", Constant.HADOOP_HOME_PATH);
//                args = new String[] { "C:/Users/Administrator/Desktop/Project/Data/phone",
//                        Constant.HOME_OUTPUT };
//            }

//        }

        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WcDriver.class);

        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
// 逻辑一样 直接用reducer 代替，在map端合并
//        job.setCombinerClass(WcCombiner.class);
//        job.setCombinerClass(WcReducer.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
