package com.techvalley.parse.common.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.techvalley.common.exception.ConfigurationException;
import com.techvalley.parse.common.QueryParameter;
import com.techvalley.parse.common.SolrQuery;
import com.techvalley.search.solr.core.DocumentTable;
import com.techvalley.search.solr.core.QueryHelper;
import com.techvalley.search.solr.core.SolrHelper;

public class SolrQueryImp implements SolrQuery{

	private Log log = LogFactory.getLog(SolrQueryImp.class);
	private SolrHelper sh;
	
	public SolrQueryImp() throws ConfigurationException, Exception{
		this.sh = new SolrHelper();
	}
	@Override
	public List<QueryParameter> query(String key, int pageSize, int curPage) {
		QueryHelper qh = new QueryHelper();
		
		//返回字段,查询参数，排序条件;
		String[] fieldList = new String[]{
				"UUID","xmmc", "fasm", "path_m", "content_m","cjsj"
		};
		String[] queryList = new String[]{
				"xmmc:*" + key + "*","fasm:*"+ key + "*","content_m:" + key
				
		};
		String[] sortList = new String[]{
				"cjsj desc"
		};
				
		log.info("key=>" + key + "pageSize=>" + pageSize + "curPage=>" + curPage);
		
		//设置查询参数；
		queryList = key == null ? null : queryList; //key=null,默认的按照cjsj排序；
		qh.query(queryList, null, fieldList, sortList);
		
		//设置高亮度
		String[] fields = new String[]{"xmmc, fasm, content_m"};
		qh.setHighlight("<font color=\"red\">", "</font>",fields , 1, 100, false);
		
		//设置分页；
		int beginIndex = curPage <= 0 ? 0 : pageSize * (curPage - 1);
		qh.setPage(beginIndex, pageSize);
		
		//查询
		DocumentTable doc= null;
		try {
			doc = sh.query(qh);
		} catch (SolrServerException e) {
			e.printStackTrace();
			return new ArrayList<QueryParameter>();
		}
		
		//解析Document中的参数为JSON格式。
		SolrDocumentList docList = doc.getDoclist();
		List<QueryParameter> params = new ArrayList<QueryParameter>();
		Map<String, Integer> pos = new HashMap<String, Integer>();
		for(int i = 0; i < docList.size(); i++){
			QueryParameter param = this.getParam(docList.get(i));
			pos.put(param.getUUID(), i);
			param.setSum(doc.getNumFound()); //添加文档数；
		    params.add(param); //按顺序添加数据；
		}
		
		//高亮显示；
		Map<String,Map<String,List<String>>> hightlight = doc.getHighlightMap();
		for(String h : hightlight.keySet()){
		    try {
				this.replaceHight(params.get(pos.get(h)), hightlight.get(h)); //利用高亮显示；
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return params;
	}
	
	private QueryParameter getParam(SolrDocument doc){
		QueryParameter q = new QueryParameter();
		
		q.setUUID(doc.get("UUID").toString());
		q.setCjsj(doc.get("cjsj").toString());
		q.setContent_m(doc.get("content_m").toString());
		q.setFasm(doc.get("fasm").toString());
		q.setPath_m(doc.get("path_m").toString());
		q.setXmmc(doc.get("xmmc").toString());
		
		log.info("query result=>" + q.getUUID()+ "\t" + q.getXmmc());
		return q;
	}

	private void replaceHight(QueryParameter q, Map<String,List<String>> light) throws  IllegalArgumentException, IllegalAccessException{
		for(String key : light.keySet()){
			Class<?> c = q.getClass();
			Field f = null;
			
			//获取值域对象；
			try {
				f = c.getDeclaredField(key);
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			}
			
			//设置高亮显示；
			if(f != null){
				f.setAccessible(true); 
				String value = light != null && light.size() > 0 ? light.get(key).get(0) : null; //替换字段；
				if(value != null){
					f.set(q, value);
					log.info("Higth=>" + q.getUUID() + " : " + f.get(q));
				}
			}
		}
	}
}
