package cn.learn.hadoop.join.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 一个产品表(pd.txt) 一个order表(order.txt)
 * 通过pd_id 进行job
 */
public class MapJoinDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:/bigDataInstall/hadoop-2.7.2");
        args = new String[]{"C:/Users/Administrator/Desktop/Project/Data" +
                "/ReduceJoin/order.txt", "C:/Users/Administrator/Desktop" +
                "/Project/Data/output"};

// 1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar包路径
        job.setJarByClass(MapJoinDriver.class);

        // 3 关联map
        job.setMapperClass(MapJoinMapper.class);

// 4 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 加载缓存数据,加上协议，默认 hdfs
        job.addCacheFile(new URI("file:///C:/Users/Administrator/Desktop/Project/Data/ReduceJoin/pd.txt"));

        // 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 8 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}

