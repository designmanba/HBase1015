package com.atgugui.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


public class HBaseDemo {

    private static Admin admin;
    private static Connection conn;

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //连接到hbase
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
//        boolean h1015 = tableExists(admin,tableName);
//        createTable("h1015","info1","info2");
//        for (int i = 0; i < 10 ; i++) {
//            putData("h1015","1001" + i,"info1","age","20" + i);
//
//        }
        getAllData("h1015");


    }

    //读取h1015中的数据
    private static void getAllData(String tableName) throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf(tableName));

        //扫描数据
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        //遍历结果
        for (Result result : results) {
            String row = Bytes.toString(result.getRow());
//            System.out.println(row);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String v = Bytes.toString(CellUtil.cloneValue(cell));
                String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String r = Bytes.toString(CellUtil.cloneRow(cell));
                System.out.println("r = " + r);
                System.out.println("cf = " + cf);
                System.out.println("c = " + c);
                System.out.println("v = " + v);
                System.out.println("-------------------------");

            }

        }

    }

    private static void putData(String tableName, String rowKey, String cf,String column, String value) throws Exception {
        //1、获取表
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));//
        //向表中 添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));


        table.put(put);



    }

    /**
     * 创建一张表
     * @param
     */
    private static void createTable(String tableName,String...cfs) throws Exception {
        //先判断一张表是否存在，
        if (tableExists(tableName)) {
            return;
        }

        //不存在就创建
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            HColumnDescriptor df = new HColumnDescriptor(cf);
            desc.addFamily(df);//向表中添加列簇


        }
        admin.createTable(desc);

    }

    /**
     * 判断表是否存在
     * @param
     * @param tableName
     * @return
     * @throws IOException
     */
    private static boolean tableExists(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }


    //向表中添加数据
}
