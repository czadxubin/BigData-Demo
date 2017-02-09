package com.xbz.bigdata.demo.MutualFriends;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author 找出互为好友的好友
 *
 */
public class EachOtherFriendsTwoJob {
	public static class EachOtherFriendsStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
		private Text outKey=new Text();
		private Text outValue=new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String[] a=value.toString().split("-");
			outKey.set(a[0]);
			outValue.set(a[1]);
			context.write(outKey, outValue);
			
			outKey.set(a[1]);
			outValue.set(a[0]);
			context.write(outKey, outValue);
		}
	}
	public static class EachOtherFriendsStepTwoReducer extends Reducer<Text,Text, NullWritable, Text>{
		private Text outValue=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			StringBuffer sb=new StringBuffer(key.toString()+":");
			while(it.hasNext()){
				sb.append(it.next().toString()+",");
			}
			outValue.set(sb.toString());
			context.write(NullWritable.get(), outValue);
		}
	}
	public static void main(String[] args) throws Exception {
		//创建Job实例
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJobName("MR实现找出相互为好友");
		job.setJarByClass(EachOtherFriendsTwoJob.class);
		
		//配置Mapper
		job.setMapperClass(EachOtherFriendsStepTwoMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//配置Reducer
		job.setReducerClass(EachOtherFriendsStepTwoReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//配置输入输出格式
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path inPath = new Path(args[0]);
		Path outpath = new Path(args[1]);
		
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
