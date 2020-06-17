package cn.learn.gulietl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper  extends Mapper<LongWritable, Text, NullWritable,Text> {

    private Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String etlStr = ETLUTil.oriString2UtilString(line);
        if (StringUtils.isBlank(etlStr)){
            return;
        }else {
            text.set(etlStr);
            context.write(NullWritable.get(),text);
        }


    }
}
