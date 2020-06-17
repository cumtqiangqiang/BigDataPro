package cn.learn.hadoop.partitionsort;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer <FlowBean,Text,Text, FlowBean>{
    @Override
    protected void reduce(FlowBean bean, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {

        for (Text phone:values) {
            context.write(phone,bean);
        }



    }
}
