package com.weibo.test;

import com.sun.tools.internal.jxc.ap.Const;
import com.weibo.constants.Constants;
import com.weibo.dao.HBaseDao;
import com.weibo.utils.HbaseUtils;

import java.io.IOException;

public class TestWeiBo {

    public static void init(){
        try {
            HbaseUtils.createNameSpace(Constants.NAMESPACE);
            HbaseUtils.createTable(Constants.CONTENT_TABLE,
                    Constants.CONTENT_TABLE_VERSIONS,Constants.CONTENT_TABLE_CF);

            HbaseUtils.createTable(Constants.INBOX_TABLE,
                    Constants.INBOX_TABLE_VERSIONS,Constants.INBOX_TABLEE_CF);

            HbaseUtils.createTable(Constants.RELATTION_TABLE,
                    Constants.RELATTION_TABLE_VERSIONS,
                    Constants.RELATTION_TABLE_CF1,
                    Constants.RELATTION_TABLE_CF2);

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
    public static void main(String[] args) throws IOException {
//            init();
        HBaseDao.publishWeibo("1001","Qiang is coming");

        HBaseDao.addAttends("1002","1001","1003");
        HBaseDao.getInit("1002");
        System.out.println("************1111*****************");
        HBaseDao.publishWeibo("1003","Tom left");
        HBaseDao.publishWeibo("1001","Fiona goto school");
        HBaseDao.publishWeibo("1003","TOM TTt goto school");
        HBaseDao.publishWeibo("1001","Fiona love Qiang");
        HBaseDao.publishWeibo("1003","QIANG QIANG go HOME");
        System.out.println("************222*****************");
        HBaseDao.deleteAttends("1002","1003");
        HBaseDao.getInit("1002");
        System.out.println("************3333*****************");
        HBaseDao.addAttends("1002","1003");
        HBaseDao.getInit("1002");
        System.out.println("************444*****************");
        HBaseDao.getInit("1001");

    }
}
