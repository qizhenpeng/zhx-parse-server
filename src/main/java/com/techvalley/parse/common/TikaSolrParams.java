package com.techvalley.parse.common;

import java.util.Map;

public class TikaSolrParams {
	private String tn;
	private String userId;
	private String rowKey;
	private String path;
	private Map<String, Object> keys;
	
	public TikaSolrParams(){
		
	}
	
	public TikaSolrParams(String tn, String userId, String rowKey, String path,
			Map<String, Object> keys) {
		super();
		this.tn = tn;
		this.userId = userId;
		this.rowKey = rowKey;
		this.path = path;
		this.keys = keys;
	}
	
	
	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getTn() {
		return tn;
	}
	public void setTn(String tn) {
		this.tn = tn;
	}
	public String getRowKey() {
		return rowKey;
	}
	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public Map<String, Object> getKeys() {
		return keys;
	}
	public void setKeys(Map<String, Object> keys) {
		this.keys = keys;
	}

}
