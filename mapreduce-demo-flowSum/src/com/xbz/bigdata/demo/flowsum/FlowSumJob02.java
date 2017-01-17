package com.xbz.bigdata.demo.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 在FlowSumJob02计算出来的结果的基础之上
 * 按照计流量排列手机号和总流量流量
 * @author 许宝众
 *
 */
public class FlowSumJob02 {
	private static FlowBean flowBean=new FlowBean(0L,0L);
	private static Text phone=new Text();
	/*
	 * Mapper传值到Reducer时，会先根据Mapper输出OUTKEY进行排序
	 */
	static class FlowSumMapper02 extends Mapper<LongWritable, Text,FlowBean , Text>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String oneLine=value.toString();
			String[] arr=oneLine.split("\t");
			flowBean.setSumFlow(Long.valueOf(arr[2]));
			phone.set(arr[0]);
			context.write(flowBean,phone);
		}
	}
	/*
	 * 输入：flowBean、手机号
	 * 输出：手机号、总流量
	 * 
	 */
	static class FlowSumReducer02 extends Reducer<FlowBean,Text, Text,LongWritable>{
		private static LongWritable sumflow=new LongWritable();
		@Override
		protected void reduce(FlowBean flowBean, Iterable<Text> phone,Context context)
				throws IOException, InterruptedException {
			sumflow.set(flowBean.getSumFlow());
			context.write(phone.iterator().next(),sumflow );
		}
	}
	public static void main(String[] args) throws Exception {
		//创建Job实例
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("流量统计--结果排序");
		job.setJarByClass(FlowSumJob02.class);
		
		//配置Mapper
		job.setMapperClass(FlowSumMapper02.class);
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		//配置Reducer
		job.setReducerClass(FlowSumReducer02.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//配置输入输出格式
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path inPath = new Path(args[0]);
		Path outpath = new Path(args[1]);
		TextInputFormat.setInputPaths(job, inPath);
		
		//调试时使用
		FileSystem fs = FileSystem.newInstance(conf);
		if(fs.exists(outpath)){
			fs.delete(outpath, true);
		}

		TextOutputFormat.setOutputPath(job, outpath);
		
		//配置输入输出文件路径
		TextInputFormat.setInputPaths(job, inPath);
		TextOutputFormat.setOutputPath(job, outpath);
		
		//提交运行,可检测执行是否正常
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
