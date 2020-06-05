package cn.learn.hadoop.udfoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class FilterRecoderWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream baiduStream;
    private FSDataOutputStream otherStream;
    public FilterRecoderWriter() {
    }
    public FilterRecoderWriter(TaskAttemptContext job) {

        FileSystem fs;
        Configuration conf = job.getConfiguration();
        try {

            fs = FileSystem.get(conf);
             Path badiduPath = new Path("C:\\Users\\Administrator\\Desktop\\Project" +
                     "\\Data\\baidulog.txt");
             Path otherPath = new Path("C:\\Users\\Administrator\\Desktop" +
                    "\\Project\\Data\\otherlog.txt");

             baiduStream = fs.create(badiduPath);
             otherStream = fs.create(otherPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        line = line + "\r\n";
        if (line.contains("baidu")){
            baiduStream.write(line.getBytes());
        }else {
            otherStream.write(line.getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(baiduStream);
        IOUtils.closeStream(otherStream);
    }
}
