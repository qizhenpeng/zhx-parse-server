package com.techvalley.parse.common;

import java.io.IOException;
import java.util.Map;

import com.techvalley.search.exception.RowKeyNullException;

/**
 * 查询HBase中的数据；
 * 利用UUID查询数据；
 * @author ling
 *
 */
public interface HBaseQuery {
	/**
	 * 利用UUID的rowkey查询数据；
	 * @param uuid
	 * @return 所有的数据；
	 * @throws RowKeyNullException 
	 * @throws IOException 
	 */
	public Map<String, String> hbaseQuery(String tableName, String uuid) throws IOException;
}
