package com.techvalley.parse.common;

import java.util.List;

public class QueryParams {
	
	private int sum;
	private List<QueryParameter> lists;
	
	public QueryParams(){
		
	}
	public QueryParams(int sum, List<QueryParameter> lists){
		this.sum = sum;
		this.lists = lists;
	}
	public int getSum() {
		return sum;
	}
	public void setSum(int sum) {
		this.sum = sum;
	}
	public List<QueryParameter> getLists() {
		return lists;
	}
	public void setLists(List<QueryParameter> lists) {
		this.lists = lists;
	}

	
}
