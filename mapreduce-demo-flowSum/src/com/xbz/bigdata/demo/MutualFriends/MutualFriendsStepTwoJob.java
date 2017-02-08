package com.xbz.bigdata.demo.MutualFriends;

import java.io.IOException;
import java.util.Arrays;
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
import org.junit.Test;

import com.xbz.bigdata.demo.join.EmployeeInfo;
import com.xbz.bigdata.demo.join.JoinJob;
import com.xbz.bigdata.demo.join.JoinJob.JoinMapper;
import com.xbz.bigdata.demo.join.JoinJob.JoinReducer;

/**
 * 找出共同好友<br>
 * 已经第一步后得到结果如下：
 * A:D,C,B,<br>
 * B:D,A,  <br>
 * C:A,D,  <br>
 * D:A,C,B,<br>
 * G:A,B,  <br>
 * H:B,C,  <br>
 * L:C,    <br>
 * M:D,    <br>
 * @author xubaozhong 
 *
 */
public class MutualFriendsStepTwoJob {
	public static class MutualFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text outKey=new Text();
		private Text outValue=new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] friend_persons = value.toString().split(":");
			String friend = friend_persons[0];
			String[] persons = friend_persons[1].split(",");
			//排序persons
			Arrays.sort(persons);
			//输出B-C：A
			outValue.set(friend);
			for (int i = 0; i < persons.length-1; i++) {
				for (int j = i+1; j < persons.length; j++) {
					outKey.set(persons[i]+"-"+persons[j]);
					context.write(outKey, outValue);
				}
			}
		}
	}
	@Test
	public void testSort(){
		String value="A:B,C,D";
		String[] friend_persons = value.split(":");
		String friend = friend_persons[0];
		String[] persons = friend_persons[1].split(",");
		//排序persons
		Arrays.sort(persons);
		//输出B-C：A
		for (int i = 0; i < persons.length-1; i++) {
			for (int j = i+1; j < persons.length; j++) {
				System.out.println(persons[i]+"-"+persons[j]+":"+friend);
				 
			}
		}
	}
	public static class MutualFriendsStepTwoReducer extends Reducer<Text, Text, NullWritable, Text>{
		private Text outValue=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb=new StringBuffer();
			sb.append(key.toString()+":");
			Iterator<Text> it = values.iterator();
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
		job.setJobName("MR实现找出相同好友第二步");
		job.setJarByClass(MutualFriendsStepTwoJob.class);
		
		//配置Mapper
		job.setMapperClass(MutualFriendsStepTwoMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//配置Reducer
		job.setReducerClass(MutualFriendsStepTwoReducer.class);
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
//					job.setPartitiTworClass(FlowSumPartitiTwor01.class);
		
		//重点是我们希望分区存放结果，我们就要保证Reduce Tasks的数量,这里的数据应该是提前根据逻辑确定下来的
		
		//提交运行,可检测执行是否正常
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
