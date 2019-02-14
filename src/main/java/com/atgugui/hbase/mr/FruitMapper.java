package com.atgugui.hbase.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;
import java.util.List;

//mapper端  读数据
public class FruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        Put put = new Put(key.get()); // 拿到行键

       // Cell[] cells = value.rawCells();
        List<Cell> cells = value.listCells();
        for (Cell cell : cells) {
            put.addColumn(
                    CellUtil.cloneFamily(cell),
                    CellUtil.cloneQualifier(cell),
                    CellUtil.cloneValue(cell));

        }

        //写出数据
        context.write(key,put);
    }
}
