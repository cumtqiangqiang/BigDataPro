package com.weibo.dao;

import com.weibo.constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDao {
    /**
     * 某用户发布 一条内容，更新到内容表，并将该内容同步到其粉丝的收件箱中
     *
     * @param uid     用户id
     * @param content 发布内容
     * @throws IOException
     */
    public static void publishWeibo(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
//       操作微博内容表
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        long ts = System.currentTimeMillis();
//        rowkey
        String rowKey = uid + "_" + ts;
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF),
                Bytes.toBytes("content"), Bytes.toBytes(content));

        contentTable.put(put);
//        操作微博收件箱表
//        1.获取该用户粉丝
        Table relationTable =
                connection.getTable(TableName.valueOf(Constants.RELATTION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        List<Put> inboxPuts = new ArrayList<>();
//        获取粉丝 列簇
        get.addFamily(Bytes.toBytes(Constants.RELATTION_TABLE_CF2));
        Result result = relationTable.get(get);

        for (Cell cell : result.rawCells()) {
//          收件箱表中的 rowkey 就是该用户的粉丝
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLEE_CF),
                    Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);


        }

//        如果有粉丝 则在收件箱表中更新
        if (inboxPuts.size() > 0) {
            Table inboxTable =
                    connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPuts);

            inboxTable.close();
        }

        contentTable.close();
        relationTable.close();
        connection.close();

    }

    /**
     * 某用户关注一个或多人
     *
     * @param uid
     * @param attends
     */
    public static void addAttends(String uid, String... attends) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relationTable =
                connection.getTable(TableName.valueOf(Constants.RELATTION_TABLE));
//假设 用户A关注B C D

        Put attendPut = new Put(Bytes.toBytes(uid));
        List<Put> relationPuts = new ArrayList<>();
        for (String attend : attends) {
//         1.  A 用户行 要在attends 列簇中添加 B C D 列
            attendPut.addColumn(Bytes.toBytes(Constants.RELATTION_TABLE_CF1),
                    Bytes.toBytes(attend), Bytes.toBytes(attend));

//       2. 同时 B C D 行都要在 fans 列簇添加 一行A
            Put fansPut = new Put(Bytes.toBytes(attend));

            fansPut.addColumn(Bytes.toBytes(Constants.RELATTION_TABLE_CF2),
                    Bytes.toBytes(uid), Bytes.toBytes(uid));

            relationPuts.add(fansPut);

        }
        relationPuts.add(attendPut);
        relationTable.put(relationPuts);

//        inboxTable 插入数据
//        A 关注B C D ,在inboxTable中 在用户A 行插入B C D列 ，value 为其最近发布的三条微博
        Table contTable =
                connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));


        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
//获取该用户的 发布的内容
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend +
                    "|"));
            ResultScanner scanner = contTable.getScanner(scan);
            long ts = System.currentTimeMillis();
            for (Result result : scanner) {
// for 循环添加，避免加入hbase 表的数据 时间戳都一致，所以put 的时候加上时间戳
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLEE_CF),
                        Bytes.toBytes(attend), ts++, result.getRow());

            }

        }
        if (!inboxPut.isEmpty()) {
            Table inboxTable =
                    connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);
            inboxTable.close();
        }
        relationTable.close();
        contTable.close();
        connection.close();
    }

    public static void deleteAttends(String uid, String... dels) throws IOException {

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relationTable =
                connection.getTable(TableName.valueOf(Constants.RELATTION_TABLE));

        List<Delete> deleteList = new ArrayList<>();
        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATTION_TABLE_CF1)
                    , Bytes.toBytes(del));

            Delete delete = new Delete(Bytes.toBytes(del));
            delete.addColumns(Bytes.toBytes(Constants.RELATTION_TABLE_CF2)
                    , Bytes.toBytes(uid));
            deleteList.add(delete);

        }
        relationTable.delete(uidDelete);
        relationTable.delete(deleteList);

//        收件箱表删除
        Delete inboxUidDelete = new Delete(Bytes.toBytes(uid));
        Table inboxTable =
                connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        for (String del : dels) {
            inboxUidDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLEE_CF)
                    , Bytes.toBytes(del));
        }
        inboxTable.delete(inboxUidDelete);
        inboxTable.close();
        relationTable.close();
        connection.close();


    }

}
