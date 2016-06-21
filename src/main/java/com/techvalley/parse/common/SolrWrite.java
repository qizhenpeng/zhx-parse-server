package com.techvalley.parse.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

/**
 * 提供Solr写入服务的接口。
 * 通过这个接口可以实现Solr的各种数据的写入,
 * @author ling
 * @version 1.0
 * Time 2015/1/29 11:57
 *
 */
public interface SolrWrite {

	void write(Map<String, String> doc) throws SolrServerException, IOException;
	
	void write(List<SolrInputDocument> doc) throws SolrServerException, IOException;
    
	void write(SolrInputDocument doc) throws SolrServerException, IOException;
}
