package com.atgugui.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class TestHabase {
    //1、判断表是否有存在
    public static boolean tableExixt(String tableName) throws IOException {
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum","192.168.6.102");
        //获取hbase管理员对象
        HBaseAdmin admin = new HBaseAdmin(configuration);
        //执行
        boolean tableExists = admin.tableExists(tableName);
        //关闭资源
        admin.close();
        return tableExists;
    }

    //创建表
    //删除表
    //增除表

    public static void main(String[] args) throws IOException {
        System.out.println(tableExixt("student"));
        System.out.println(tableExixt("stu"));
    }
}
