package com.atgugui.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


public class HbaseTest02 {

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




    //创建表
    public static void createTable(String tableName,String ...cfs) throws IOException {
        admin = connection.getAdmin();

        //判断表是否存在
        if(admin.tableExists(TableName.valueOf(tableName))){
            return;
        }
        //表描述器
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列簇
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            desc.addFamily(hColumnDescriptor);
        }
        admin.createTable(desc);

        System.out.println("创建表成功");


    }

    //创建命名空间

    public static void createNameSpace(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin();

        //判断nameSpace是否存在
        if (nameSpaceExists(nameSpace)) {
            return;
        }


        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        //执行创建命名空间操作
        admin.createNamespace(namespaceDescriptor);
        System.out.println("namespace successful!");

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

    //向表中添加数据
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        //向表中添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));

        //执行添加数据的操作
        table.put(put);


    }

    //扫描数据
    public static void scanData(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建扫面对象
        Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);

        //拿到所有的数据
        for (Result result : results) {
            String row = Bytes.toString(result.getRow());
            System.out.println("外层："+row);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }


        }

    }

    //获取指定列簇的数据

    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(cf));


        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rk="+Bytes.toString(CellUtil.cloneRow(cell)));

        }


    }


    public static void main(String[] args) throws IOException {
        //创建表
        createTable("myns:h1015","info1","info2");
        //创建命名空间
        //createNameSpace("myns");
        //添加数据
        //putData("myns:h1015","1001","info1","name","lisi");
        //putData("myns:h1015","1001","info1","age","18");
        //("myns:h1015","1002","info2","name","zhangsan");
        //putData("myns:h1015","1002","info2","name","sangwu");
        //putData("myns:h1015","1002","info2","sex","femal");

        scanData("myns:h1015");

    }


}
