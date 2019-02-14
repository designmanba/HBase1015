package com.atgugui.hbase.weibo;

import org.apache.hadoop.hbase.TableName;
import java.io.IOException;
public interface IWeiboBusiness {
    String WEIBO_NS = "weibo";
    TableName TBL_CONTENT = TableName.valueOf(WEIBO_NS + ":tbl_content");
    TableName TBL_RELATION = TableName.valueOf(WEIBO_NS + ":tbl_relation");
    TableName TBL_INBOX = TableName.valueOf(WEIBO_NS + ":tbl_inbox");

    //定义用到的列簇
    String  CF_CONTENT = "content";
    String  CF_ATTENDED = "attended";
    String  CF_FANS = "fans";
    String  CF_INFO = "info";
    /**
     * 创建命名空间
     */
    void createNamespace() throws IOException;

    /**
     *
     创建内容表
     */
    void createContentTable();


    /**
     * 创建关系表
     */

    void createRelationTable();

    /**
     * 创建收件箱表
     */
    void createInBoxTable();

    /**
     * @param uid 主动关注的人
     * @param attendedUid  被关注的人
     */
    void attendOther(String uid,String attendedUid) throws IOException;

    /**
     * 发布一条微博
     * @param uid 发微博的人
     * @param content 发布微博的内容
     */
    void publishBlog(String uid,String content) throws IOException;

    /**
     * 查看我的 fans
     * @param uid
     */
    void viewMyFans(String uid);

    /**
     * 查看我的微博
     * @param uid
     */
    void viewMyBlog(String uid);

}
