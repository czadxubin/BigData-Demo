package com.xbz.bigdata.mapreduce.demo.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 统计单词数量的Reduce Task<br>
 * KEYIN：Map Task的输出参数Key<br>
 * VALUEIN：Map Task的输出参数Value<br>
 * KEYOUT：Reducer自定义输出Key<br>
 * VALUEOUT：Reducer自定义输出Value<br>
 * 以上参数均需要实现 <code>org.apache.hadoop.io.Writable</code><br>
 * @author 许宝众
 */
public class WordCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text word, Iterable<IntWritable> valuesIterator,Context context)throws IOException, InterruptedException {
		int count=0;
		//统计单词数量
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
