package com.xbz.bigdata.demo.maxItem;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner泛型 key value与mapper输出一致
 * @author xubaozhong
 *
 */
public class MaxItemPartitioner extends Partitioner<ItemBean, NullWritable>{

	@Override
	public int getPartition(ItemBean key, NullWritable value, int numPartitions) {
		
		return (key.getItemId().hashCode() & Integer.MAX_VALUE)%numPartitions;
	}

}
