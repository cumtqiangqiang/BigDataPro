package cn.learn.hadoop.topN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortGroupingComparator extends WritableComparator {


    protected OrderSortGroupingComparator(){
        super(OrderBean.class, true);
    }


    /**
     * 重写这个方法没有效果 public int compare(Object a, Object b) {？？？？？？
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable  b) {
        OrderBean order1 = (OrderBean)a;
        OrderBean order2 = (OrderBean)b;

        int result;
        if (order1.getOrderId() > order2.getOrderId()) {
            result = 1;
        } else if (order1.getOrderId() < order2.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }

        return result;
    }

}
