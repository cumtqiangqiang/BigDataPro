package com.weibo.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Constants {
// 全局常量
    public static final Configuration CONFIGURATION =
            HBaseConfiguration.create();

    public static final String NAMESPACE = "weibo";
//    微博内容表
    public static final String CONTENT_TABLE = "weibo:content";
    public static final String CONTENT_TABLE_CF = "info";
    public static final int CONTENT_TABLE_VERSIONS = 1;
// 用户关系表
    public static final String RELATTION_TABLE= "weibo:relation";
    public static final String RELATTION_TABLE_CF1 = "attends";
    public static final String RELATTION_TABLE_CF2 = "fans";
    public static final int RELATTION_TABLE_VERSIONS = 1;
    //    收件箱表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final String INBOX_TABLEE_CF = "info";
    public static final int INBOX_TABLE_VERSIONS = 2;

}
