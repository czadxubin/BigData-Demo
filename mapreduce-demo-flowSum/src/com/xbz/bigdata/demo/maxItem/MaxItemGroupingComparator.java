package com.xbz.bigdata.demo.maxItem;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MaxItemGroupingComparator extends WritableComparator{
	public MaxItemGroupingComparator() {
		//必须有,且第二个参数设置为true
		super(ItemBean.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ItemBean aa = (ItemBean)a;
		ItemBean bb = (ItemBean)b;
		return aa.getItemId().compareTo(bb.getItemId());
	}
}
