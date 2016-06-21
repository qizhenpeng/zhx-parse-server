package com.techvalley.parse.common;

import java.util.List;
import java.util.Map;

public interface ParserDelete {

	/**
	 * 删除多个UUID数据，这些数据包括HDFS上的文件，HBase上的记录和Solr上的记录；
	 * @param uuids uuid集合；
	 * @return 
	 */
	public Map<String, ParseParameter> parseDelete(List<String> uuids);
	
}
