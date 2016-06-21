package com.techvalley.parse.common;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.fs.Path;


/**
 * 提供数据解析服务器的业务流程；
 * @author ling
 * @version 1.0
 * Time 2015/1/29 12:20
 *
 */
public interface ParserServer {

	/**
	 * 解析数据到Hbase，Solr中；
	 * @param path HDFS路径；
	 * @param keys 要写入到Solr中的额外数据；
	 * @param absSum 摘要行数；
	 * @param sw 写入到SolrWrite中的对象；
	 * @param hbase HBase定位信息。Note ： 必须含有三个数，tableName，rowKeys， familyName；
	 * @return 返回的参数；
	 */
	public ParseParameter parserHBaseSolr(Path path, Map<String, Object> keys, int absSum, SolrWrite sw, String...hbase);
	
	public ParseParameter parserAndSolr(File file, Map<String, Object> keys, int absSum, SolrWrite sw);
}
