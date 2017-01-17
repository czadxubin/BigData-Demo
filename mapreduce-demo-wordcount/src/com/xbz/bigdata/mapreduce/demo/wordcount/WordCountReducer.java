package com.xbz.bigdata.mapreduce.demo.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ͳ�Ƶ���������Reduce Task<br>
 * KEYIN��Map Task���������Key<br>
 * VALUEIN��Map Task���������Value<br>
 * KEYOUT��Reducer�Զ������Key<br>
 * VALUEOUT��Reducer�Զ������Value<br>
 * ���ϲ�������Ҫʵ�� <code>org.apache.hadoop.io.Writable</code><br>
 * @author ����
 */
public class WordCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text word, Iterable<IntWritable> valuesIterator,Context context)throws IOException, InterruptedException {
		int count=0;
		//ͳ�Ƶ�������
		Iterator<IntWritable> iterator = valuesIterator.iterator();
		while(iterator.hasNext()){
			count+=iterator.next().get();
		}
//		for (IntWritable valueInterator : valuesIterator) {
//			count++;
//		}
		context.write(word, new IntWritable(count));
	}
}
