package cn.learn.hadoop.udfpartition;
import common.utils.ConfigurationManager;
import common.utils.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * 统计每个手机号的总流量，自定义bean。
 */
public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (ConfigurationManager.getBoolean(Constant.HADOOP_RUN_LOCAL)){

            System.setProperty("hadoop.home.dir", Constant.HADOOP_COMPANY_PATH);
            args = new String[] { "C:/Users/Administrator/Desktop/Project/Data/phone",
                    "C:/Users/Administrator/Desktop/Project/Data/output" };
        }


        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//        5 个分区
//        job.setPartitionerClass(ProvincePartitioner.class);
//        job.setNumReduceTasks(5);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
