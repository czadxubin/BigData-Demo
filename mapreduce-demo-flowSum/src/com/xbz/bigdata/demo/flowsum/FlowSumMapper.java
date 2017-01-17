package com.xbz.bigdata.demo.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 输出KEY：手机号
 * 输出VALUE：自定义的实现Writable接口的描述结果的对象
 * @author 许宝众
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	@Override
	protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		try{
			String oneLine=value.toString();
			String[] arr=oneLine.split("\\|");
			if(arr.length>=3){//过滤无效的数据
				String phoneNum=arr[0];
				Long uploadFlow=Long.valueOf(arr[1]);
				Long dFlow=Long.valueOf(arr[2]);
				FlowBean flowBean = new FlowBean(uploadFlow,dFlow);
				context.write(new Text(phoneNum), flowBean);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
