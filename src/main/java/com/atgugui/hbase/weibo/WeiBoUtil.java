package com.atgugui.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * 工具类，写一些普通的方法
 * 创建命名空间
 */
public class WeiBoUtil {

    //连接Hbase
    private static Connection connection = null;
    private static Admin admin = null;
    private static Configuration conf=null;

    static {
        conf = HBaseConfiguration.create();
        //配置zookeeper
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 创建命名空间
     * @param nameSpace
     * @throws IOException
     */

    public static void createNameSpace(String nameSpace)  {
        try {
            admin = connection.getAdmin();
            //判断nameSpace是否存在
            if (nameSpaceExists(nameSpace)) return;
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            //执行创建命名空间操作
            admin.createNamespace(namespaceDescriptor);
            System.out.println("namespace successful!");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //判断namespace 是否存在
    public  static boolean nameSpaceExists(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.getNamespaceDescriptor(nameSpace);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 关闭资源
     */

    public static void close(Connection connection,Admin admin,Table table){

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * 获取表
     * @param tableName
     * @return
     */
    public static Table getTable(TableName tableName){
        try {
            return connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 创建表
      * @param tableName 表名
     * @param splitRegion 预分区
     * @param versions 每个列簇储存最多的版本
     * @param cfs //多个列
     */

    public static void createTable(TableName tableName,byte[][] splitRegion,int versions,String ...cfs)  {
            //获取admin
        try {
            admin = connection.getAdmin();
            //判断表是否存在
            if(admin.tableExists(tableName)) return;
            //表描述器
            HTableDescriptor table = new HTableDescriptor(tableName);
            //添加列簇
            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setMaxVersions(versions);
                table.addFamily(hColumnDescriptor);
            }
            //执行创建表的操作
            admin.createTable(table,splitRegion);
            admin.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("创建表成功.....");
    }


}












