package com.atgugui.hbase.weibo;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        WeiboBusinessImp weiboBusinessImp = new WeiboBusinessImp();

        //关注功能实现
        //weiboBusinessImp.attendOther("1001","1002");
       // weiboBusinessImp.attendOther("1001","1003");
        //发微博测试
      //weiboBusinessImp.publishBlog("1002","1002--> this is my fourBlog");
       //查找fans
        //weiboBusinessImp.viewMyFans("1002");
      //  weiboBusinessImp.viewMyBlog("1002");

        weiboBusinessImp.attendOther("1001","1002");
    }
}
