package com.atgugui.hbase.mr02;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitTool implements Tool {


    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();
        //设置主类
        job.setJarByClass(FruitTool.class);

        //设置输入路劲
        FileInputFormat.setInputPaths(job, "hdfs://hadoop102:9000/fruit.tsv");

        //设置Mapper
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置reducer
        TableMapReduceUtil.initTableReducerJob("fruit", FruitReducer.class, job);

        //提交
        boolean result = job.waitForCompletion(true);


        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    //程序入口
    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new FruitTool(), args);
        System.out.println(code == 0 ? "执行成功" : "执行失败");

    }
}
