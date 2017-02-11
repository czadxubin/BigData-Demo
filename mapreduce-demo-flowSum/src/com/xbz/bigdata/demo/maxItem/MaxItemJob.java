package com.xbz.bigdata.demo.maxItem;

import java.io.IOException;

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

import com.xbz.bigdata.demo.join.JoinJob.JoinReducer;

public class MaxItemJob {
	
	public static class MaxItemMapper extends Mapper<LongWritable, Text, ItemBean, NullWritable>{
		private ItemBean bean=new ItemBean();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] a = value.toString().split(",");
			bean.setItemId(a[0]);
			bean.setItem_money(Float.valueOf(a[2]));
			context.write(bean, NullWritable.get());
		}
	}
	public static class MaxItemReducer extends Reducer<ItemBean, NullWritable, NullWritable, Text>{
		private Text outValue=new Text();
		@Override
		protected void reduce(ItemBean bean, Iterable<NullWritable> values,
				Context context)
				throws IOException, InterruptedException {
			String res=bean.getItemId()+"\t"+bean.getItem_money();
			outValue.set(res);
			context.write(NullWritable.get(), outValue);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		//创建Job实例
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf);
				job.setJobName("统计每个订单中单商品交易金额最大值");
				job.setJarByClass(MaxItemJob.class);
				
				//配置Mapper
				job.setMapperClass(MaxItemMapper.class);
				job.setMapOutputKeyClass(ItemBean.class);
				job.setMapOutputValueClass(NullWritable.class);
				
				//配置Reducer
				job.setReducerClass(MaxItemReducer.class);
				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(Text.class);
				
				//配置输入输出格式
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				
				//设置进入Reducer阶段的分组逻辑
				job.setGroupingComparatorClass(MaxItemGroupingComparator.class);
				//设置分区策略，保证同一个订单号进入同一个reducer
				job.setPartitionerClass(MaxItemPartitioner.class);
				
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
//				job.setPartitionerClass(FlowSumPartitioner01.class);
				
				//重点是我们希望分区存放结果，我们就要保证Reduce Tasks的数量,这里的数据应该是提前根据逻辑确定下来的
//				job.setNumReduceTasks(FlowSumPartitioner01.partionMap.size());
				
				//提交运行,可检测执行是否正常
				System.exit(job.waitForCompletion(true)?0:1);
	}
}
