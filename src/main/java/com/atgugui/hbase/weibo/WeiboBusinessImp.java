package com.atgugui.hbase.weibo;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WeiboBusinessImp implements IWeiboBusiness {
  private DecimalFormat format =  new DecimalFormat("00");
    private final Table tbl_relation;
    private final Table tbl_content;
    private final Table tbl_inbox;

    /**
     * 初始化
     */
    public WeiboBusinessImp() throws IOException {
        createNamespace();
        createContentTable();
        createRelationTable();
        createInBoxTable();

        //获取表
         tbl_relation = WeiBoUtil.getTable(TBL_RELATION);
         tbl_content = WeiBoUtil.getTable(TBL_CONTENT);
         tbl_inbox = WeiBoUtil.getTable(TBL_INBOX);
    }

    /**
     * 创建命名空间
     *
     * @throws IOException
     */
    @Override
    public void createNamespace() throws IOException {
        WeiBoUtil.createNameSpace(WEIBO_NS);

    }

    /**
     * 创建content表
     */
    @Override
    public void createContentTable() {
        WeiBoUtil.createTable(TBL_CONTENT,
                new byte[][]{
                        Bytes.toBytes("01|"),
                        Bytes.toBytes("02|"),
                        Bytes.toBytes("03|"),
                        Bytes.toBytes("04|"),
                        Bytes.toBytes("05|"),
                        Bytes.toBytes("06|"),
                        Bytes.toBytes("07|"),
                        Bytes.toBytes("08|"),
                        Bytes.toBytes("09|"),
                        Bytes.toBytes("10|"),
                        Bytes.toBytes("11|"),
                        Bytes.toBytes("12|"),
                },
                1,
                CF_CONTENT);
    }

    /**
     * 创建RelationTable 关系表
     */
    @Override
    public void createRelationTable() {
        WeiBoUtil.createTable(TBL_RELATION,
                null,
                1,
                CF_ATTENDED,CF_FANS);
    }

    /**
     * 创建InBoxTable 收件箱
     */
    @Override
    public void createInBoxTable() {
        WeiBoUtil.createTable(
                TBL_INBOX,
                null,
                3,
                CF_INFO);
    }

    /**
     * 关注功能实现
     * @param uid 主动关注的人
     * @param attendedUid  被关注的人
     * @throws IOException
     */
    @Override
    public void attendOther(String uid, String attendedUid) throws IOException {
        /**
         *1、在用户关系表中添加两列：
         * uid 这一行添加一列 ：attended:attendedUid ->null
         * attendedUid 这一行添加一列：fans:uid ->null
         *
         * 2、在收件箱
         *         uid 这一行 添加attendedUid 的微博
         *
         */
        putDataToTable(tbl_relation,uid,CF_ATTENDED,attendedUid,null);
        putDataToTable(tbl_relation,attendedUid,CF_FANS,uid,null);

        //1001（uid） 关注了1002(attendedId) 在收件箱1001这一行添加1002所有的微博
        // 查看 attended 最新的3条微博
        //1、先找到1002 的微博    11_1002_  倒着去扫描
        int count = 0;
       http: for (int i = 11;i >=1;i--){
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(format.format(i) + "_" +attendedUid +"_"));
            scan.setStopRow(Bytes.toBytes(format.format(i) + "_" + attendedUid + "_|"));

            ResultScanner scanner = tbl_content.getScanner(scan);
            List<byte[]> rks = new ArrayList<>();
            for (Result result : scanner) {
                rks.add(result.getRow());
            }
            //反转
            Collections.reverse(rks);
            for (byte[] rk : rks) {
                count ++;
                //添加到 收件箱1001这一行
                putDataToTable(tbl_inbox,uid,CF_INFO,attendedUid,Bytes.toString(rk));
                if (count == 3) break http;
            }
        }
    }

    //发表 微博
    @Override
    public void publishBlog(String uid, String content) throws IOException {
        /**
         * 1、向tbl_content表中插入:
         *     rowKey   02_uid_ts
         *
         *     cf:column content:info ->具体的内容
         *
         *   2、给粉丝收件箱推送刚刚发布的这条微博
         *    1002 发布一条消息
         *    a:先找到1002粉丝 去用户关系表
         *    rowKey  1002(uid) fans列簇下所有列 每人发一份
         *
         *    b:找到粉丝 是1001
         *
         *    rowkey 1001
         *    info:1002 value:微博的rowkey
         */
        String rowKey;
        putDataToTable(tbl_content,
               rowKey= getTblContentRowKey(uid),
                CF_CONTENT,
                "info",
                content);

        //找到uid的所有粉丝，在关系表中找到uid这一行fans这一列簇的所有列
        Get get = new Get(uid.getBytes());
        get.addFamily(Bytes.toBytes(CF_FANS));
        Result result = tbl_relation.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            //向tbl_inbox表中写入数据 ：rowkey :粉丝的id
            putDataToTable(tbl_inbox,
                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                    CF_INFO,
                    uid,
                    rowKey);
        }

    }

    /**
     * 查看我的fans
     * @param uid
     */
    @Override
    public void viewMyFans(String uid) {
        System.out.println(uid + "fans is:");
        List<String> myFans = findMyFans(uid);
        for (String myFan : myFans) {
            System.out.println("--" + myFan);
        }

    }

    /**
     * 查看我的微博
     * @param uid
     */
    @Override
    public void viewMyBlog(String uid) {
        System.out.println(uid + "myBlog have:");
        List<String> myBlog = findMyBlog(uid);
        for (String s : myBlog) {
            System.out.println("---" +s);
        }

    }

    /**
     * 查找指定uid 的微博
     * @param uid
     * @return
     */
    public  List<String> findMyBlog(String uid){
        List<String> blogs = new ArrayList<>();
        try {
            ResultScanner scanner = tbl_content.getScanner(new Scan());
            for (Result result : scanner) {
                if(Bytes.toString(result.getRow()).contains("_"+uid+"_")){
                    Cell cell = result.listCells().get(0);//因为每行只有一列，所以不用遍历
                    blogs.add(Bytes.toString(result.getRow()) + "\t" + Bytes.toString(CellUtil.cloneValue(cell)));
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return blogs;
    }


    /**
     * 找到指定id的粉丝
     * @param uid
     * @return
     */
    public List<String> findMyFans(String uid) {
        List<String> fans = new ArrayList<>();

        try {
            Get get = new Get(Bytes.toBytes(uid));
            get.addFamily(Bytes.toBytes(CF_FANS));
            Result result = tbl_relation.get(get);
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                fans.add(Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fans;
    }

    /**
     * 计算内容每行数据rowKey
     * @param uid
     * @return
     */
    private String getTblContentRowKey(String uid){
        //02_uid_ts
        LocalDate now = LocalDate.now();
        int month = now.getMonth().getValue();
        String monthStr = month < 10 ? "0" + month : month + "";
        return  monthStr + "_" + uid + "_" + System.currentTimeMillis();
    }

    /**
     * 向指定表中插入数据
     */
    public void putDataToTable(Table table,String rowKey,String cf,String qualifier,String value){
        try {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(qualifier),Bytes.toBytes(value == null ? "" : value));
        table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
