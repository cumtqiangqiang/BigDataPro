package cn.learn.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyUDF extends UDF {

    public int evaluate(int data){
        return data+5;
    }

    /**
     * 通过重载的方式 可变参
     * @param data
     * @param data1
     * @return
     */
    public int evaluate(int data,int data1){
        return data+data1+5;
    }
}
