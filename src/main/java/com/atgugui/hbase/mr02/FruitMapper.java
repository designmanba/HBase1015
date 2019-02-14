package com.atgugui.hbase.mr02;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//将文件中的数据传入到hbase 上
// 1001    Apple    Red
public class FruitMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //每行数据使用/t 切割

        String[] split = value.toString().split("\t");

        //根据数组中的数据分别取值

        String rowKey = split[0];
        String name = split[1];
        String color = split[2];

        //初始化rowKey
        ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

        //初始化put

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addImmutable(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(name));
        put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("color"),Bytes.toBytes(color));
        context.write(rowKeyWritable,put);

    }
}
