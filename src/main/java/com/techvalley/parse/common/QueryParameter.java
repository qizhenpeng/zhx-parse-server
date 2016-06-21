package com.techvalley.parse.common;

/**
 * 查询后的参数列表；
 * @author ling
 * 
 *
 */
public class QueryParameter {
	
	private String UUID;
	private String xmmc;
	private String fasm;
	private String content_m;
	private String path_m;
	private String cjsj;
	private int sum;
	
	public QueryParameter(){
		//空的构造函数：
	}

	public QueryParameter(String UUID, String xmmc, String fasm,
			String content_m, String path_m, String cjsj, int sum) {
		super();
		this.UUID = UUID;
		this.xmmc = xmmc;
		this.fasm = fasm;
		this.content_m = content_m;
		this.path_m = path_m;
		this.cjsj = cjsj;
		this.sum = sum;
	}

	public String getUUID() {
		return UUID;
	}

	public void setUUID(String UUID) {
		this.UUID = UUID;
	}

	public String getXmmc() {
		return xmmc;
	}

	public void setXmmc(String xmmc) {
		this.xmmc = xmmc;
	}

	public String getFasm() {
		return fasm;
	}

	public void setFasm(String fasm) {
		this.fasm = fasm;
	}

	public String getContent_m() {
		return content_m;
	}

	public void setContent_m(String content_m) {
		this.content_m = content_m;
	}

	public String getPath_m() {
		return path_m;
	}

	public void setPath_m(String path_m) {
		this.path_m = path_m;
	}

	public String getCjsj() {
		return cjsj;
	}

	public void setCjsj(String cjsj) {
		this.cjsj = cjsj;
	}

	
	public int getSum() {
		return sum;
	}

	public void setSum(int sum) {
		this.sum = sum;
	}

	@Override
	public String toString() {
		return "UUID=>" + UUID + ", xmmc>=" + xmmc + ", fasm>="
				+ fasm + ", content_m>=" + content_m + ", path_m>=" + path_m
				+ ", cjsj=>" + cjsj;
	}

	
	
}
