package com.opencassandra.v01.bean;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.IndexOperator;

public class ColumnQueryDefination {
	private String name = "";
	private ByteBuffer value;
	private IndexOperator indexOperator;
	
	public ColumnQueryDefination(){}
	
	public ColumnQueryDefination(String name, ByteBuffer value, IndexOperator indexOperator){
		this.name = name;
		this.value = value;
		this.indexOperator = indexOperator;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public ByteBuffer getValue() {
		return value;
	}
	public void setValue(ByteBuffer value) {
		this.value = value;
	}
	public IndexOperator getIndexOperator() {
		return indexOperator;
	}
	public void setIndexOperator(IndexOperator indexOperator) {
		this.indexOperator = indexOperator;
	}
}
