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

import com.xbz.bigdata.demo.join.EmployeeInfo;
import com.xbz.bigdata.demo.join.JoinJob;
import com.xbz.bigdata.demo.join.JoinJob.JoinMapper;
import com.xbz.bigdata.demo.join.JoinJob.JoinReducer;

/**
 * 找出共同好友<br>
 * 原始输入文本：<br>
 * A:B,C,D,G<br>
 * B:A,G,D,H<br>
 * C:A,H,D,L<br>
 * D:B,A,C,M<br>
 * @author xubaozhong 
 *
 */
public class MutualFriendsStepOneJob {
	/**
	 *第一步Mapper<br>
	 *输出&lt;好友,人,人...&gt;<br>
	 *
	 */
	public static class MutualFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text outKey=new Text();
		private Text outValue=new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] person_friends = value.toString().split(":");
			String person=person_friends[0];
			String[] friends=person_friends[1].split(",");
			outValue.set(person);
			for (String friend : friends) {
				outKey.set(friend);
				context.write(outKey, outValue);
			}
		}
	}
	/**
	 *第一步Reducer<br>
	 *输入:key在values中是好友
	 */
	public static class MutualFriendsStepOneReducer extends Reducer<Text, Text, NullWritable, Text>{
		private Text outValue=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb=new StringBuffer();
			Iterator<Text> it = values.iterator();
			sb.append(key.toString()+":");
			while (it.hasNext()) {
				Text person =  it.next();
				sb.append(person.toString()+",");
			}
			outValue.set(sb.toString());
			context.write(NullWritable.get(), outValue);
		}
	}
	
	public static void main(String[] args) throws Exception {
		//创建Job实例
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJobName("MR实现找出相同好友");
		job.setJarByClass(MutualFriendsStepOneJob.class);
		
		//配置Mapper
		job.setMapperClass(MutualFriendsStepOneMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//配置Reducer
		job.setReducerClass(MutualFriendsStepOneReducer.class);
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
		
		//自定义part输出
//					job.setPartitionerClass(FlowSumPartitioner01.class);
		
		//重点是我们希望分区存放结果，我们就要保证Reduce Tasks的数量,这里的数据应该是提前根据逻辑确定下来的
//					job.setNumReduceTasks(FlowSumPartitioner01.partionMap.size());
		
		//提交运行,可检测执行是否正常
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
