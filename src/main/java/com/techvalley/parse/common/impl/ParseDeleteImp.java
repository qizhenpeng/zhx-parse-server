package com.techvalley.parse.common.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServerException;

import com.techvalley.common.exception.ConfigurationException;
import com.techvalley.parse.common.ParseParameter;
import com.techvalley.parse.common.ParserDelete;
import com.techvalley.parse.common.ParserServerConfig;
import com.techvalley.search.hbase.core.HBaseHelper;
import com.techvalley.search.solr.core.SolrHelper;

public class ParseDeleteImp implements ParserDelete{

	private static Log log = LogFactory.getLog(ParseDeleteImp.class);
	
	private FileSystem fs;
	private Configuration conf;
	private HBaseHelper<String> hb;
	private SolrHelper sh;
	public ParseDeleteImp() throws ConfigurationException, Exception{
		this.conf = ParserServerConfig.HDFS_CONF;
		this.fs = FileSystem.get(conf);
		this.hb = new HBaseHelper<String>();
		this.sh = new SolrHelper();
	}
	
	public Map<String, ParseParameter> parseDelete(List<String> uuids) {
		Map<String, String> data = new HashMap<String, String>();
		Map<String, ParseParameter> params = new HashMap<String, ParseParameter>();
		
		//删除多个uuid;
		if(uuids == null || uuids.size() == 0){
		   params.put("no_data", this.createPar("500", "No date to delete", data));
		   return params;
		}
		for(String uuid : uuids){
			params.put(uuid, this.parseDelete(uuid));
		}
		
		return params;
	}

	public ParseParameter parseDelete(String uuid){
		
		log.info("Begin to Delete the uuid=>" + uuid);
		Map<String, String> data = new HashMap<String, String>();
		
		if(uuid == null || "".equals(uuid) || "null".equals(uuid)){
			return this.createPar("500", "the " + uuid + " is invalid.", data);
		}
		//删除hdfs上的文件；
		try {
			this.hdfsDelete(uuid);
		} catch (IOException e) {
			e.printStackTrace();
			return this.createPar("500", e.getMessage(), data);
		}
		
		//删除hbase上的文件；
		try {
			this.hbaseDelete(uuid);
		} catch (IOException e) {
			e.printStackTrace();
			return this.createPar("500", e.getMessage(), data);
		}
		
		//删除solr上的数据；
		try {
			this.solrDelete(uuid);
		} catch (SolrServerException e) {
			e.printStackTrace();
			return this.createPar("500", e.getMessage(), data);
		} catch (IOException e) {
			e.printStackTrace();
			return this.createPar("500", e.getMessage(), data);
		}
		
		log.info("Delete with uuid=>" + uuid + " success.");
		
		return this.createPar("200", "success", data);
	}
	
	public void hdfsDelete(String uuid) throws IOException{
		log.info("Begin to Delete the HDFS with uuid=>" + uuid);
		
		//验证文件是否存在；
		this.fs = this.fs == null ? FileSystem.get(conf) : this.fs;
		String str = ParserServerConfig.HDFS_BASE_PATH + "/" + uuid;
		Path path = new Path(str);
		if(!this.fs.exists(path) && this.fs.isFile(path)){
			throw new FileNotFoundException("the file with uuid=>" + uuid + " not found.");
		}
		
		//删除文件；
		this.fs.delete(path, true); //Note: ture:如果Path上有文件也删除.
		
		log.info("Delete the HDFS with uuid=>" + uuid + " success.");
	}
	
	public void hbaseDelete(String rowKey) throws IOException{
		log.info("Begin to Delete The HBase with rowKey=>" + rowKey);
		
		//删除数据；
		hb.deleteRow(ParserServerConfig.HBASE_TABLE_NAME, rowKey);
		
		log.info("Delete The HBase with rowKey=>" + rowKey + " success.");
	}
	
	public void solrDelete(String uuid) throws SolrServerException, IOException{
		log.info("Begin to Delete The Solr with uuid=>" + uuid);
		
		//删除solr上的数据；
		this.sh.removeDoc(uuid);
		
		log.info("Delete the Solr with uuid=>" + uuid + " success.");
	}
	
	private ParseParameter createPar(String status, String msg, Map<String, String> data){
		
		log.info("Create parser parameter status=>" + status + " msg=>" + msg + " data=>" + data.toString());
		
		return new ParseParameter(status, msg, data);
	}
}
