package com.hadoop.mapreduce.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartioner extends Partitioner<Tq, IntWritable> {

    /**
     *
     * @param tq
     * @param intWritable
     * @param numPartitions  分区数量，与job中的reducetask数量一致，job.setNumReduceTasks(2)设置
     * @return
     */
    @Override
    public int getPartition(Tq tq, IntWritable intWritable, int numPartitions) {

        return tq.getYear()% numPartitions;
    }
}
