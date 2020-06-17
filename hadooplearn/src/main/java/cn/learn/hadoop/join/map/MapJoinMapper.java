package cn.learn.hadoop.join.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

import javax.print.DocFlavor;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class MapJoinMapper extends Mapper<LongWritable, Text, Text,
        NullWritable>  {

    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI uri = context.getCacheFiles()[0];
        String path = uri.getPath().toString();
        FileInputStream fileInputStream = new FileInputStream(path);
        InputStreamReader streamReader =
                new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader);

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
//            01	小米
            String[] fields = line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }

        reader.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
// 1 获取一行
        String line = value.toString();

        // 2 截取
        String[] fields = line.split("\t");

        // 3 获取产品id
        String pId = fields[1];

        // 4 获取商品名称
        String pdName = pdMap.get(pId);

        // 5 拼接
        k.set(line + "\t"+ pdName);

        // 6 写出
        context.write(k, NullWritable.get());

    }
}
