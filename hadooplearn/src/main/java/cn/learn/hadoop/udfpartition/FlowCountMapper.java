package cn.learn.hadoop.udfpartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text,
        FlowBean> {
    FlowBean bean = new FlowBean();
    Text phone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
//        1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String[] fileds = line.split("\t");
        String phoneNum = fileds[1];
        long upFlow = Long.parseLong(fileds[fileds.length-3]);
        long downFlow = Long.parseLong(fileds[fileds.length-2]);
        long sumFlow= upFlow + downFlow;
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        bean.setSumFlow(sumFlow);
        phone.set(phoneNum);
        context.write(phone,bean);

    }
}
