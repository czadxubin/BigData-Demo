package com.xbz.bigdata.mapreduce.demo.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**	
 * ͳ�Ƶ���������Map Task<br>
 * KEYIN��mapreduceÿ����һ�����ݵ���ʼƫ����<br>
 * VALUEIN��mapreduce�����һ������<br>
 * KEYOUT���û��Զ������Key<br>
 * VALUEOUT���û��Զ������Value<br>
 * ���ϲ�������Ҫʵ�� <code>org.apache.hadoop.io.Writable</code><br>
 * @author ����
 */
public class WordCountMaper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
		//����һ������
		String oneLine=value.toString().replaceAll("\\s+", " ");
		String[] words=oneLine.split(" ");
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
	
}
