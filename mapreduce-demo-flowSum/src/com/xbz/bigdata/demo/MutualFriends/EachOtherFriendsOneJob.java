package com.xbz.bigdata.demo.MutualFriends;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text.Comparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import com.xbz.bigdata.demo.MutualFriends.MutualFriendsStepOneJob.MutualFriendsStepOneMapper;
import com.xbz.bigdata.demo.MutualFriends.MutualFriendsStepOneJob.MutualFriendsStepOneReducer;

/**
 * 
 * @author 找出互为好友的好友
 *
 */
public class EachOtherFriendsOneJob {
	/**
	 *mapper第一步
	 *A:B,D,C 
	 *将其变为：
	 *输出为
	 *AB:A
	 *AD:A
	 *AC:A
	 *再来一行数据
	 *B:A,D,C
	 *输出
	 *AB:B
	 *BC:B
	 *BD:B
	 *计数
	 *AB>1即为相互是好友了
	 */
	public static class EachOtherFriendsStepOneMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private Text outKey=new Text();
		private IntWritable outValue=new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] person_friends=value.toString().split(":");
			String person=person_friends[0];
			String[] friends=person_friends[1].split(",");
			for(String friend:friends){
				if(person.compareTo(friend)>0){
					outKey.set(friend+"-"+person);
				}else{
					outKey.set(person+"-"+friend);
				}
				context.write(outKey, outValue);
			}
		}
	}
	public static class EachOtherFriendsStepOneReducer extends Reducer<Text,IntWritable, NullWritable, Text>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			StringBuffer sb=new StringBuffer();
			int count=0;
			for (IntWritable value : values) {
				count++;
			}
			if(count==2){
				context.write(NullWritable.get(), key);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		//创建Job实例
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJobName("MR实现找出互相为好友");
		job.setJarByClass(EachOtherFriendsOneJob.class);
		
		//配置Mapper
		job.setMapperClass(EachOtherFriendsStepOneMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置mapper到reducer的value值合并
		job.setCombinerKeyGroupingComparatorClass(WritableComparator.class);
		//配置Reducer
		job.setReducerClass(EachOtherFriendsStepOneReducer.class);
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
