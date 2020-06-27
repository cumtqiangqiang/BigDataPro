package com.weibo.utils;

import com.weibo.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseUtils {

    public static void createNameSpace(String nameSpace) throws IOException {
        Connection connection =
                ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
// builder 设计模式
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(descriptor);
        admin.close();
        connection.close();


    }

    private static boolean isTableExist(String tableName) throws IOException {
        Connection connection =
                ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return exists;
    }

    public static void createTable(String tableName,int versions,String...cfs) throws IOException {
        if (cfs.length <= 0){
            System.out.println("请设置列簇信息");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName +  "已经存在");
            return;
        }

        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();

        HTableDescriptor tableDescriptor =
                new HTableDescriptor(TableName.valueOf(tableName));

        for (String cf : cfs) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            columnDescriptor.setMaxVersions(versions);
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor);
        admin.close();
        connection.close();


    }


}
