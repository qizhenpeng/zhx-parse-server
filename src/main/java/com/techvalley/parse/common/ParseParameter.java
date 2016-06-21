package com.techvalley.parse.common;

import java.util.Map;

public class ParseParameter {
       
	private String status;
	private String msg;
	private Map<String, String> data; 
	
	public ParseParameter(){
		//
	}
  	
	public ParseParameter(String status, String msg, Map<String, String> data){
		this.status = status;
		this.msg = msg;
		this.data = data;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public Map<String, String> getData() {
		return data;
	}

	public void setData(Map<String, String> data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return "status:" + this.status + " msg:" + this.msg + " data:" + this.data;
	}
	
	
}