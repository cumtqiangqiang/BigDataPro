package cn.learn.hbase;

import constance.Constance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestAPI {
    private static Connection connection = null;
    private static Admin admin = null;

    static {
//        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", Constance.ZK_SERVERS);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void close(){
        if (admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean isTableExist(String tableName) throws IOException {

        boolean tableExist = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        return tableExist;
    }

    /**
     * 创建表
     * @param tableName
     * @param cls  列簇
     * @throws IOException
     */
    private static void createTable(String tableName,String ...cls) throws IOException {
        if (cls.length == 0){
            System.out.println("Please input column family");
            return;
        }
        try {
            if (isTableExist(tableName)){
                System.out.println("表已经存在");
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cl : cls) {
//            创建列簇描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cl);
//            添加列簇
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);


    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    private static void dropTable(String tableName) throws IOException {
        try {
            if (!isTableExist(tableName)){
                System.out.println(tableName + " 表不存在");
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        先disable 表
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));


    }

    /**
     * 创建命名空间
     * @param ns
     */
    private static void createNameSpace(String ns){
        NamespaceDescriptor nsDescriptor =
                 NamespaceDescriptor.create(ns).build();

        try {
            admin.createNamespace(nsDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已经存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     * @param
     * @throws IOException
     */

    private static void putData(String tableName,String rowKey,String cf,
                               String cn,String value) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn),
                Bytes.toBytes(value));
        table.put(put);

        connection.close();

    }
    private static void getData(String tableName,String rowKey,String cf,
                                String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addFamily(Bytes.toBytes(cf));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        Result result = table.get(get);
//        result.rawCells() 获取该行数据的所有cell
        for (Cell cell : result.rawCells()) {
            System.out.println("CF: "+Bytes.toString(CellUtil.cloneFamily(cell)) +
                    " CN: "+Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    " Value: "+Bytes.toString(CellUtil.cloneValue(cell)));
        }

    }



    public static void main(String[] args) throws IOException {

//        System.out.println(isTableExist("stu3"));
//
//        createTable("stu3","info");
//
//        System.out.println(isTableExist("stu3"));
//
//        dropTable("stu3");
//        putData("stu2","1001","info","name","Hello");
        getData("stu2","1001","info","name");
        close();

    }
}
