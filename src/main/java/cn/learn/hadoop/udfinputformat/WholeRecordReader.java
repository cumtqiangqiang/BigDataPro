package cn.learn.hadoop.udfinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader {

    private Configuration conf = new Configuration();
    private FileSplit fileSplit = new FileSplit();
    private boolean isProgress = true;
    private BytesWritable value = new BytesWritable();
    private Text k = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;
            conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress){
            // 1 定义缓存区
            byte[] contents = new byte[(int)this.fileSplit.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                // 2 获取文件系统
                Path path = this.fileSplit.getPath();
                fs = path.getFileSystem(this.conf);
                // 3 读取数据
                fis = fs.open(path);
                // 4 读取文件内容
                IOUtils.readFully(fis,contents,0,contents.length);
                // 5 输出文件内容
                this.value.set(contents,0,contents.length);
                // 6 获取文件路径及名称
                String name = fileSplit.getPath().toString();
                // 7 设置输出的key值
                this.k.set(name);
            }catch (Exception e){

            }finally {
                IOUtils.closeStream(fis);
            }
            isProgress = true;
            return  true;

        }

        return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
