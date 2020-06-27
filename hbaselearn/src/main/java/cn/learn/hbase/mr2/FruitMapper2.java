package cn.learn.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FruitMapper2 extends Mapper<LongWritable, Text,
        ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String rowKey = fields[0];
        String cn1V = fields[1];
        String cn2V = fields[2];
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),
                Bytes.toBytes(cn1V));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),
                Bytes.toBytes(cn2V));
        ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

        context.write(rowKeyWritable,put);


    }
}
