package cn.learn.hadoop.join.reduce;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> tableBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();
        for (TableBean bean : values) {
            if (bean.getFlag().equals("order")) {
                TableBean orderBean = new TableBean();
                try {
                    BeanUtils.copyProperties(orderBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                tableBeans.add(orderBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (TableBean bean : tableBeans) {
            bean.setPname(pdBean.getPname());
            context.write(bean, NullWritable.get());
        }

    }
}
