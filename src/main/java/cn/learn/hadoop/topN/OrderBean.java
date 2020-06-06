package cn.learn.hadoop.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int  orderId;
    private double orderPrice;

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getOrderPrice() {
        return orderPrice;
    }

    public void setOrderPrice(double orderPrice) {
        this.orderPrice = orderPrice;
    }

    public OrderBean() {
    }


    public OrderBean(int orderId, double orderPrice) {
        this.orderId = orderId;
        this.orderPrice = orderPrice;
    }




    @Override
    public int compareTo(OrderBean o) {
        int result = 0;
        if (this.orderId > o.orderId){
            result = 1;
        }else if (this.orderId < o.orderId){
            result = -1;
        }else {
            result = this.orderPrice > o.getOrderPrice() ? -1 : 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.orderId);
        out.writeDouble(this.orderPrice);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       this.orderId = in.readInt();
       this.orderPrice = in.readDouble();
    }
    @Override
    public String toString() {
        return orderId + "\t" + orderPrice;
    }

}
