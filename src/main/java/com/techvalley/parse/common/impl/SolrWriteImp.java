package com.techvalley.parse.common.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.techvalley.common.exception.ConfigurationException;
import com.techvalley.parse.common.SolrWrite;
import com.techvalley.search.solr.core.SolrHelper;

public class SolrWriteImp implements SolrWrite{

	private SolrHelper solr;
	
	public SolrWriteImp() throws ConfigurationException, Exception{
		this.solr = new SolrHelper();
	}
	
	@Override
	public void write(Map<String, String> doc) throws SolrServerException, IOException {
		
		//写入数据到Solr中。
		this.solr.addDoc(doc);
	}

	@Override
	public void write(List<SolrInputDocument> doc) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(SolrInputDocument doc) {
		
	}

}
