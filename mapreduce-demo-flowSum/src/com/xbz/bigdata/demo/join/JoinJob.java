package com.xbz.bigdata.demo.join;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.xbz.bigdata.demo.flowsum.FlowBean;
import com.xbz.bigdata.demo.flowsum.FlowSumJob01;
import com.xbz.bigdata.demo.flowsum.FlowSumMapper;
import com.xbz.bigdata.demo.flowsum.FlowSumReducer01;

/**
 * 使用MapReduce实现一个SQL里面的join
 * @author 许宝众
 *
 */
public class JoinJob {
	/**
	 * 核心思想：mapper要以两个表的公共数据：部门id作为输出key 
	 *
	 */
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, EmployeeInfo>{
		private EmployeeInfo employeeInfo=new EmployeeInfo();
		private Text outputKey=new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String[] str=value.toString().split(",");
			//部门
			if("department.data".equals(fileSplit.getPath().getName())){
				employeeInfo.setFlag("1");
				employeeInfo.setD_id(str[0]);
				employeeInfo.setD_name(str[1]);
				//没有值的字段要填充""
				employeeInfo.setE_id("");
				employeeInfo.setE_name("");
			}else{//员工
				employeeInfo.setFlag("0");
				employeeInfo.setE_id(str[0]);
				employeeInfo.setE_name(str[1]);
				employeeInfo.setD_id(str[2]);
				//没有值的字段要填充""
				employeeInfo.setD_name("");
			}
			outputKey.set(employeeInfo.getD_id());
			context.write(outputKey, employeeInfo);
		}
	}
	/**
	 *<code>NullWritable</code>不输出key
	 */
	public static class JoinReducer extends Reducer<Text, EmployeeInfo, NullWritable, Text>{
		private Text outputValue=new Text();
		@Override
		protected void reduce(Text key, Iterable<EmployeeInfo> values,
				Context context)
				throws IOException, InterruptedException {
			String d_name="";
			List<EmployeeInfo> list=new LinkedList<EmployeeInfo>();
			//先找到部门名称
			for (EmployeeInfo employeeInfo : values) {
				EmployeeInfo copyEmployeeInfo = new EmployeeInfo();
				try {
					BeanUtils.copyProperties(copyEmployeeInfo, employeeInfo);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if("1".equals(employeeInfo.getFlag())){
					d_name=employeeInfo.getD_name();
				}else{
					list.add(copyEmployeeInfo);
				}
			}
			//将所有人员添加上部门名称
			for (EmployeeInfo employeeInfo : list) {
				employeeInfo.setD_name(d_name);
				outputValue.set(employeeInfo.toString());
				context.write(NullWritable.get(), outputValue);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		//创建Job实例
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf);
				job.setJobName("MR实现SQL的join的demo");
				job.setJarByClass(JoinJob.class);
				
				//配置Mapper
				job.setMapperClass(JoinMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(EmployeeInfo.class);
				
				//配置Reducer
				job.setReducerClass(JoinReducer.class);
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
//				job.setPartitionerClass(FlowSumPartitioner01.class);
				
				//重点是我们希望分区存放结果，我们就要保证Reduce Tasks的数量,这里的数据应该是提前根据逻辑确定下来的
//				job.setNumReduceTasks(FlowSumPartitioner01.partionMap.size());
				
				//提交运行,可检测执行是否正常
				System.exit(job.waitForCompletion(true)?0:1);
	}

}
