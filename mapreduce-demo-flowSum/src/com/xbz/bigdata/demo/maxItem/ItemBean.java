package com.xbz.bigdata.demo.maxItem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.math.IEEE754rUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ItemBean implements WritableComparable{
	/**订单ID**/
	private String itemId;
	/**订单金额**/
	private Float item_money;
	
	public String getItemId() {
		return itemId;
	}
	public void setItemId(String itemId) {
		this.itemId = itemId;
	}
	public Float getItem_money() {
		return item_money;
	}
	public void setItem_money(Float item_money) {
		this.item_money = item_money;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(itemId);
		out.writeFloat(item_money);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		itemId=in.readUTF();;
		item_money=in.readFloat();
	}
	@Override
	public int compareTo(Object o) {
		ItemBean other = (ItemBean)o;
		//首先按照订单排序，升序
		int res=itemId.compareTo(other.getItemId());
		if(res==0){
			//首先按照订单排序，降序
			res= - item_money.compareTo(other.getItem_money());
		}
		return res;
	}
	
	
}
