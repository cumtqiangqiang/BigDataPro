package cn.learn.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将 fruit 表里的name 列导入 fruit2 表
 */
public class FruitDriver implements Tool {

    private Configuration conf;
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(FruitDriver.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(args[0],scan,FruitMapper1.class
                , ImmutableBytesWritable.class, Put.class,job);

        TableMapReduceUtil.initTableReducerJob(args[1],FruitReducer1.class,
                job);
        boolean result = job.waitForCompletion(true);

        return result ? 0:1;
    }

    @Override
    public void setConf(Configuration conf) {
         this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf ;
    }

    public static void main(String[] args) {

        args = new String[2];
        args[0] = "fruit";
        args[1] = "fruit2";
        try {
//            使用HBaseConfiguration并在resources 里加入hbase-site.xml， 可以使得在本地运行，不用jar
            Configuration configuration = HBaseConfiguration.create();
            ToolRunner.run(configuration,new FruitDriver(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
