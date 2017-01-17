package com.xbz.bigdata.demo.flowsum;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * KEY/VALUE为Mapper的输出.
 * 截取手机号的前三位作为part文件的标识
 * @author 许宝众
 *
 */
public class FlowSumPartitioner01 extends Partitioner<Text, FlowBean> {
	public static Map<String,Integer> partionMap=new HashMap<>();
	static {
		partionMap.put("134", 0);
		partionMap.put("135", 1);
		partionMap.put("136", 2);
		partionMap.put("137", 3);
		partionMap.put("138", 4);
		partionMap.put("139", 5);
	}
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String part = key.toString().substring(0,3);
		Integer partInt = partionMap.get(part);
		if(partInt==null){
			partInt=0;
		}
		return partInt%numPartitions;
	}

}
