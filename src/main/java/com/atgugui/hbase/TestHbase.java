package com.atgugui.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class TestHbase {
    private static  Admin admin = null;
    private  static  Connection connection = null;
   private static Configuration configuration = null;

    static {

       configuration = HBaseConfiguration.create();
        //配置zookeeper地址
        configuration.set("hbase.zookeeper.quorum ","hadoop102");


        //获取连接对象
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void close(Connection connection,Admin admin){
        if(connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    //判断表是否存在
    public static boolean ExistTable(String tableName) throws Exception {


        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        //调用admin里面的方法
      //  boolean tableExists = admin.tableExists(tableName);
        admin.close();
        return tableExists;
    }

    //创建表
    public static void createTable(String tableName, String... cfs) throws Exception {

        if(ExistTable(tableName)){
            System.out.println("表已经存在");
            return;
        }

         //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //添加列簇
        for (String cf : cfs) {
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);

        }

        //创建表操作
        admin.createTable(hTableDescriptor);
        System.out.println("表创建成功");

    }

    //删除表
    public  static  void deleteTable(String tableName) throws Exception {
        if (!ExistTable(tableName)){
            return;
        }

        //使表不可用
        admin.disableTable(TableName.valueOf(tableName));

        //执行删除操作
        admin.deleteTable(TableName.valueOf(tableName));

        System.out.println("表已删除！！！");

    }

    //增/删

    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //获取表对象
       // HTable table = new HTable(configuration, TableName.valueOf(tableName));


        //创建put 对象  一个put对象对应以一个rowKey
        Put put = new Put(Bytes.toBytes(rowKey));
        //添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        table.put(put);
        table.close();
        System.out.println("添加数据成功");

    }

    //删除炒作
    public static void delete(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //获取delete对象  一个delete对象对应一个rowKey
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //只能删除最新的版本   老版本删除不了
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //删除所有版本
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //执行删除操作
        table.delete(delete);
        System.out.println("删除成功");
        table.close();

    }

    //查
    //全表扫描
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //构建扫描器对象
        Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("rk=" + Bytes.toString(CellUtil.cloneRow(cell))
                +",cf=" + Bytes.toString(CellUtil.cloneFamily(cell))
                +",cn=" + Bytes.toString(CellUtil.cloneQualifier(cell))
                +",va=" + Bytes.toString(CellUtil.cloneValue(cell)));

            }

        }
        System.out.println("扫描完成");
        table.close();

    }

    //获取指定的列簇
    public static void getDate (String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        //获取get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //指定到列
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        //指定版本
       // get.setMaxVersions();

        //获取数据操作
        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rk=" + Bytes.toString(CellUtil.cloneRow(cell))
                    +",cf=" + Bytes.toString(CellUtil.cloneFamily(cell))
                    +",cn=" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",va=" + Bytes.toString(CellUtil.cloneValue(cell)));

        }
        table.close();


    }

    public static void main(String[] args) throws Exception {
        //判断h1015是否存在
        //System.out.println(ExistTable("h1015"));
        //System.out.println(ExistTable("student"));
       // System.out.println(ExistTable("hhah"));
       // createTable("friend","info");

       // deleteTable("friend");
        //System.out.println(ExistTable("friend"));

//        putData("student","1003","info","name","dada");
//        putData("student","1003","info","age","18");
//        putData("student","1003","info","sex","male");
        //delete("student","1003","info","name");
       // scanTable("h1015");
        getDate("student","1003","info","age");

        close(connection,admin);


    }
}
