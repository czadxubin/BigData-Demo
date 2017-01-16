package com.xbz.bigdata.mapreduce.demo.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**	
 * 统计单词数量的Map Task<br>
 * KEYIN：mapreduce每读入一行数据的起始偏移量<br>
 * VALUEIN：mapreduce读入的一行数据<br>
 * KEYOUT：用户自定义输出Key<br>
 * VALUEOUT：用户自定义输出Value<br>
 * 以上参数均需要实现 <code>org.apache.hadoop.io.Writable</code><br>
 * @author 许宝众
 */
public class WordCountMaper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		//读入一行数据
		String oneLine=value.toString().replaceAll("\\s+", " ");
		String[] words=oneLine.split(" ");
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
	
}
