package com.ascend.connect.sqlserver;

public class Query {
	
	private String query;
	private String operation;
	private String primaryKey;
	
	public Query(String query, String operation, String primaryKey) {
		super();
		this.query = query;
		this.operation = operation;
		this.primaryKey = primaryKey;
	}
		
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}

	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
}
