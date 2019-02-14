package com.atgugui.hbase.mr;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitTool implements Tool {


    private Configuration conf;


 @Override
    public int run(String[] strings) throws Exception {

     //获取job 对象
     Job job = Job.getInstance();

     //指定driver类
     job.setJarByClass(FruitTool.class);

     //指定mapper，和输入
     TableMapReduceUtil.initTableMapperJob(
             "fruit.tsv",
             new Scan(),
             FruitMapper.class,
             ImmutableBytesWritable.class,
             Put.class,job);


     //指定reduce 和输出
     TableMapReduceUtil.initTableReducerJob("fruit",FruitReuducer.class,job);

     //提交
     boolean result = job.waitForCompletion(true);

     return  result ? 0 : 1;
    }

    //设置配置文件
    @Override
    public void setConf(Configuration configuration) {
        this.conf = conf;

    }

    //获取配置文件
    @Override
    public Configuration getConf() {
        return this.conf;
    }


    //程序入口
    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new FruitTool(), args);
        if (code == 0){
            System.out.println("任务正常完成");

        }else {
            System.out.println("任务失败");
        }
    }
}
