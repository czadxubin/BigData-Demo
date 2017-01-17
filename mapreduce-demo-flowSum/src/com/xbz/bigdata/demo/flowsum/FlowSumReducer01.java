package com.xbz.bigdata.demo.flowsum;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 统计用户的总的上行流量、下行流量和总流量使用情况
 * @author 许宝众
 *
 */
public class FlowSumReducer01 extends Reducer<Text,FlowBean,Text,Text>{
	@Override
	protected void reduce(Text text, Iterable<FlowBean> iterable,Context context)throws IOException, InterruptedException {
		try {
			Iterator<FlowBean> iterator = iterable.iterator();
			FlowBean sumFlowBean = new FlowBean(0L,0L);
			while(iterator.hasNext()){
				FlowBean flowBean = iterator.next();
				sumFlowBean.add(flowBean);
			}
			String res =sumFlowBean.getUploadFlow()+"\t"+sumFlowBean.getdFlow()+"\t"+sumFlowBean.getSumFlow();
			context.write(text, new Text(res));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
