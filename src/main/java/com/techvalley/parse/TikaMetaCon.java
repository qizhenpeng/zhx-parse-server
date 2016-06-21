package com.techvalley.parse;

import java.util.Map;

public class TikaMetaCon {
	
	public String name; 
	public String path; 
	public Suffix suffix; 
	public Map<String, Object> keys; 
	public String content; 
	public TikaMeta meta; 
	
	public TikaMetaCon(){
	}

	public TikaMetaCon(String name, String path, Suffix suffix, Map<String, Object> keys, String content, TikaMeta meta){
		this.name = name;
		this.path = path;
		this.suffix = suffix;
		this.keys = keys;
		this.content = content;
		this.meta = meta;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Suffix getSuffix() {
		return suffix;
	}

	public void setSuffix(Suffix suffix) {
		this.suffix = suffix;
	}
	
	public TikaMeta getMeta() {
		return meta;
	}

	public void setMeta(TikaMeta meta) {
		this.meta = meta;
	}

	public Map<String, Object> getKeys() {
		return keys;
	}

	public void setKeys(Map<String, Object> keys) {
		this.keys = keys;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	
	
}
