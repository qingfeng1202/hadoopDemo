package com.hadoop.mapreduce.tq;

import com.hadoop.mapreduce.MyWordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MyTQ {

    // 1949-10-01 14:21:02	34c
    public static class Tmapper extends Mapper<LongWritable, Text, Tq, IntWritable>{

        private final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        private final static Tq tkey = new Tq();
        private final static IntWritable tvalue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取时间 温度数组
            String[] words = StringUtils.split(value.toString(), '\t');

            // 处理日期
            LocalDate parse = LocalDate.parse(words[0], dateTimeFormatter);
            tkey.setYear(parse.getYear());
            tkey.setMonth(parse.getMonthValue());
            tkey.setDay(parse.getDayOfMonth());

            // 处理温度
            int wd = Integer.parseInt(words[1].replace("c", ""));
            tkey.setWd(wd);

            tvalue.set(wd);

            context.write(tkey, tvalue);
        }
    }

    public static class Treducer extends Reducer<Tq, IntWritable, Text, IntWritable>{

        private final static Text tkey = new Text();
//        IntWritable tval = new IntWritable();

        @Override
        protected void reduce(Tq key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int flag = 0;
            int day = 0;

            for (IntWritable val : values){
                if(flag == 0){
                    tkey.set(key.toString());
//                tval.set(val.get());
                    context.write(tkey, val);
                    flag ++;
                    day = key.getDay();
                }

                if(flag != 0 && day != key.getDay()){
                    tkey.set(key.toString());
                    context.write(tkey, val);
                    break;
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        // 1.配置
        //创建Configuration
        Configuration conf = new Configuration();

        conf.set("mapreduce.app-submission.corss-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        //创建Job
        Job job = Job.getInstance(conf);
        // 设置job名字
        job.setJobName("tq");
        //设置job的处理类
        job.setJarByClass(MyWordCount.class);

        // 2.设置输入输出路径
        Path path = new Path("/tq/input");
        FileInputFormat.addInputPath(job, path);
        Path outPath = new Path("/tq/output");
        if(outPath.getFileSystem(conf).exists(outPath)){
            outPath.getFileSystem(conf).delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 3.设置Mapper
        job.setMapperClass(Tmapper.class);
        job.setMapOutputKeyClass(Tq.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4.自定义排序比较器
        job.setSortComparatorClass(TsortComparator.class);

        // 5.自定义分区器
        job.setPartitionerClass(TPartioner.class);

        // 6.自定义组排序
        job.setGroupingComparatorClass(TGroupComparator.class);

        // 7.设置reducetask数量
        job.setNumReduceTasks(2);
        // 8.设置reducer
        job.setReducerClass(Treducer.class);

        // 9.提交作业
        job.waitForCompletion(true);
    }
}
