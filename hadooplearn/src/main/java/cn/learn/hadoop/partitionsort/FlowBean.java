package cn.learn.hadoop.partitionsort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现可序列换的bean
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private  long upFlow;
    private long downFlow;
    private long sumFlow;
    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }
    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
    public long getUpFlow() {
        return upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }
    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }


    @Override
    public String toString() {
        return  upFlow + "\t" +
                downFlow + "\t" +
                sumFlow;
    }

    @Override
    public int compareTo(FlowBean bean) {
        int sort = 0;
        if (this.sumFlow > bean.sumFlow ){
           sort = -1;
        }else  if (this.sumFlow < bean.sumFlow){
            sort = 1;
        }

        return sort;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }
}
