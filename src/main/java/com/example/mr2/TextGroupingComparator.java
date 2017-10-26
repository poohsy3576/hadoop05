package com.example.mr2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextGroupingComparator extends WritableComparator {

	protected TextGroupingComparator() {
		super(Text.class, true);
	}
	
	//super : ascending sorting logic
	//-super : descending
	public int compare(WritableComparable a, WritableComparable b) {
		return -super.compare(a, b);
	}
	
}
