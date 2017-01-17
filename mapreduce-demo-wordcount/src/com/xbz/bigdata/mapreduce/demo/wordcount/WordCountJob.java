package com.xbz.bigdata.mapreduce.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * ��hadoop����mapreduce��׼������
 * 
 */
public class WordCountJob {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// ����һ��Jobʵ��
		Job job=Job.getInstance(conf,"wordcount");
		
		/*
		 * ��������jar
		 * ��Ϊ���ǻὫ����Ϊjar������ָ���Լ���Class��
		 */
		job.setJarByClass(WordCountJob.class);
	
		// ����Maper��
		job.setMapperClass(WordCountMaper.class);
		// ����Maper���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// ����Reducer��
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// ���������ʽ|�����ʽ
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		/* 
		 * ָ������·��|���·��
		 * API�ƺ��б仯
		 * ԭʼΪ�� 
		 * job.setInputPath(new Path(args[0])));
		 * job.setOutputPath(new Path(args[1]));
		 * ��ǰ�汾
		 */
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// �ύ���񣬵ȴ�����������˳�
		// ִ�гɹ�����0��ʧ�ܷ���1�������ڼ������״̬
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
