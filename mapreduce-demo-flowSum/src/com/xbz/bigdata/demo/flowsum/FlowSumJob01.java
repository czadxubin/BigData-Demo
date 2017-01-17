package com.xbz.bigdata.demo.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 根据手机号对用户进行流量使用统计，结果根据手机号的区段分类输出
 * @author 许宝众
 *
 */
public class FlowSumJob01 {
	public static void main(String[] args) throws Exception {
		//创建Job实例
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("流量统计--区段分区");
		job.setJarByClass(FlowSumJob01.class);
		
		//配置Mapper
		job.setMapperClass(FlowSumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//配置Reducer
		job.setReducerClass(FlowSumReducer01.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
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
		
		//自定义part输出
//		job.setPartitionerClass(FlowSumPartitioner01.class);
		
		//重点是我们希望分区存放结果，我们就要保证Reduce Tasks的数量,这里的数据应该是提前根据逻辑确定下来的
//		job.setNumReduceTasks(FlowSumPartitioner01.partionMap.size());
		
		//提交运行,可检测执行是否正常
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
