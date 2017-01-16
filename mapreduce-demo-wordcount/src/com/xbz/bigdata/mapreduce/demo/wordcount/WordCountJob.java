package com.xbz.bigdata.mapreduce.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 在hadoop上跑mapreduce的准备工作
 * 
 */
public class WordCountJob {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// 创建一个Job实例
		Job job=Job.getInstance(conf,"wordcount");
		
		/*
		 * 设置所在jar
		 * 因为我们会将其打包为jar，所以指定自己的Class类
		 */
		job.setJarByClass(WordCountJob.class);
	
		// 设置Maper类
		job.setMapperClass(WordCountMaper.class);
		// 设置Maper输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 设置Reducer类
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 设置输入格式|输出格式
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		/* 
		 * 指定输入路径|输出路径
		 * API似乎有变化
		 * 原始为： 
		 * job.setInputPath(new Path(args[0])));
		 * job.setOutputPath(new Path(args[1]));
		 * 当前版本
		 */
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 提交任务，等待任务处理完成退出
		// 执行成功返回0，失败返回1。可用于检查运行状态
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
