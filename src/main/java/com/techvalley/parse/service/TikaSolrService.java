package com.techvalley.parse.service;

import com.techvalley.parse.common.HBaseQuery;
import com.techvalley.parse.common.ParseParameter;
import com.techvalley.parse.common.ParserDelete;
import com.techvalley.parse.common.ParserServer;
import com.techvalley.parse.common.ParserServerConfig;
import com.techvalley.parse.common.QueryParameter;
import com.techvalley.parse.common.SolrQuery;
import com.techvalley.parse.common.SolrWrite;
import com.techvalley.parse.common.StatusSingle;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.annotation.Resource;

import org.apache.hadoop.fs.Path;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service("tikaSolrService")
public class TikaSolrService {

	@Resource(name = "threadPoolTaskExecutor")
	private ThreadPoolTaskExecutor executor;

	@Resource(name = "solrWrite")
	private SolrWrite sw;

	@Resource(name = "parserServer")
	private ParserServer parser;

	@Resource(name = "solrQuery")
	private SolrQuery sq;

	@Resource(name = "hbaseQuery")
	private HBaseQuery hb;

	@Resource(name = "parserDelete")
	private ParserDelete pd;
	private StatusSingle sta = new StatusSingle();

	public void submit(Path path, Map<String, Object> keys, int absSum,
			String rowKey) {
		parseAndSolr(path, keys, absSum, rowKey);
	}

	public ParseParameter getStatus(String key) {
		return this.sta.getStatus(key);
	}

	public List<QueryParameter> query(String key, int pageSize, int curPage) {
		return this.sq.query(key, pageSize, curPage);
	}

	public Map<String, String> query(String tableName, String uuid) {
		try {
			return this.hb.hbaseQuery(tableName, uuid);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new HashMap();
	}

	public Map<String, ParseParameter> delete(List<String> uuids) {
		return this.pd.parseDelete(uuids);
	}

	private void parseAndSolr(Path path, Map<String, Object> keys, int absSum,
			String rowKey) {
		ParseAndSolr par = new ParseAndSolr(path, keys, absSum,
				ParserServerConfig.HBASE_TABLE_NAME, rowKey,
				ParserServerConfig.HBASE_FAMILY);
		Future future = this.executor.submit(par);

		this.sta.submit(keys.get("tn").toString(), future);
	}

	private class ParseAndSolr implements Callable<ParseParameter> {
		private Path path;
		private Map<String, Object> keys;
		private int absSum;
		private String tableName;
		private String rowkey;
		private String familyName;

		public ParseAndSolr(Path path, Map<String, Object> keys, int absSum,
				String tableName, String rowKey, String familyName) {
			this.path = path;
			this.keys = keys;
			this.absSum = absSum;
			this.tableName = tableName;
			this.rowkey = rowKey;
			this.familyName = familyName;
		}

		public ParseParameter call() throws Exception {
			return TikaSolrService.this.parser
					.parserHBaseSolr(this.path, this.keys, this.absSum,
							TikaSolrService.this.sw, new String[] {
									this.tableName, this.rowkey,
									this.familyName });
		}
	}
}