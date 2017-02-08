package com.xbz.bigdata.demo.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class EmployeeInfo implements Writable{
	
	/**员工id**/
	private String e_id;
	/**员工姓名**/
	private String e_name;
	/**部门id**/
	private String d_id;
	/**部门名称**/
	private String d_name;
	/**标识是员工(0)、部门(1)**/
	private String flag;

	public String getE_id() {
		return e_id;
	}

	public void setE_id(String e_id) {
		this.e_id = e_id;
	}

	public String getE_name() {
		return e_name;
	}

	public void setE_name(String e_name) {
		this.e_name = e_name;
	}

	public String getD_id() {
		return d_id;
	}

	public void setD_id(String d_id) {
		this.d_id = d_id;
	}

	public String getD_name() {
		return d_name;
	}

	public void setD_name(String d_name) {
		this.d_name = d_name;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}
	@Override
	public String toString(){
		return e_id+","+e_name+","+d_name;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(e_id);
		out.writeUTF(e_name);
		out.writeUTF(d_id);
		out.writeUTF(d_name);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		e_id=in.readUTF();
		e_name=in.readUTF();
		d_id=in.readUTF();
		d_name=in.readUTF();
		flag=in.readUTF();
	}

}
