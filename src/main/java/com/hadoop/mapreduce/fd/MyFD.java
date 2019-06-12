package com.hadoop.mapreduce.fd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFD {

    public static void main(String[] args) throws Exception {
        //创建Configuration
        Configuration configuration = new Configuration();

        //创建Job
        Job job = Job.getInstance(configuration);

        //设置job的处理类
        job.setJarByClass(MyFD.class);
        job.setJobName("friend");

        // 设置输入输出路径
        Path inPath = new Path("/fd/input");
        FileInputFormat.addInputPath(job, inPath);

        Path outPath = new Path("/fd/output");
        if(outPath.getFileSystem(configuration).exists(outPath)){
            outPath.getFileSystem(configuration).delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //设置map相关参数
        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce相关参数
        job.setReducerClass(FReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }


}
