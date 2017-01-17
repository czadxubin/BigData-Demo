package com.xbz.bigdata.demo.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	/**上行流量**/
	private Long uploadFlow;
	/**下行流量**/
	private Long dFlow;
	/**总流量**/
	private Long sumFlow;
	//反序列化时，需要反射调用空参构造函数，所以要显示定义一个
	public FlowBean() {
	}
	public FlowBean(Long uploadFlow, Long dFlow) {
		this.uploadFlow = uploadFlow;
		this.dFlow = dFlow;
		this.sumFlow = uploadFlow+dFlow;
	}

	public Long getUploadFlow() {
		return uploadFlow;
	}

	public void setUploadFlow(Long uploadFlow) {
		this.uploadFlow = uploadFlow;
	}

	public Long getdFlow() {
		return dFlow;
	}

	public void setdFlow(Long dFlow) {
		this.dFlow = dFlow;
	}

	public Long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(uploadFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		uploadFlow=in.readLong();
		dFlow=in.readLong();
		sumFlow=in.readLong();
	}

	public FlowBean add(FlowBean other){
		this.dFlow+=other.getdFlow();
		this.uploadFlow+=other.getUploadFlow();
		this.sumFlow=uploadFlow+dFlow;
		return this;
	}
	@Override
	public int compareTo(FlowBean o) {
		return (int) (o.getSumFlow()-this.sumFlow);
	}
}
