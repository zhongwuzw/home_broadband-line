package com.opencassandra.v01.dao.factory;

import org.apache.cassandra.thrift.IndexOperator;

public class QueryExpression {
	
	private String columnName = ""; 
	private IndexOperator op = null;
	private String value = "";
	
	public QueryExpression(String columnName, IndexOperator op, String value){
		this.columnName = columnName;
		this.op = op;
		this.value = value;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public IndexOperator getOp() {
		return op;
	}

	public void setOp(IndexOperator op) {
		this.op = op;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
}
