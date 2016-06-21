package com.techvalley.parse.common;

import java.util.List;

public interface SolrQuery {
	
	/**
	 * 查询solr服务器的内容，如果key=null， 按时间排序，如果key！=null，查询按照时间分页。
	 * @param key 查询键
	 * @param pageSize 页的大小
	 * @param curPage 当前页数。
	 * @return QueryParameter的内容；
	 */
	public List<QueryParameter> query(String key, int pageSize, int curPage);

}
